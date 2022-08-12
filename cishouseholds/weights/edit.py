from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from cishouseholds.derive import assign_named_buckets
from cishouseholds.edit import update_column_values_from_map


def join_on_existing(df: DataFrame, df_to_join: DataFrame, on: List):
    """
    Join 2 dataframes on columns in 'on' list and
    override empty values in the left dataframe with values from the right
    dataframe.
    """
    columns = [col for col in df_to_join.columns if col in df.columns]
    for col in columns:
        if col not in on:
            df_to_join = df_to_join.withColumnRenamed(col, f"{col}_FT")
    df = df.join(df_to_join, on=on, how="left")
    for col in columns:
        if col not in on:
            df = df.withColumn(col, F.coalesce(F.col(f"{col}_FT"), F.col(col))).drop(f"{col}_FT")
    return df


def fill_nulls(column_name_to_update, fill_value: int = 0):
    """Fill Null and NaN values with a constant integer."""
    return F.when((column_name_to_update.isNull()) | (F.isnan(column_name_to_update)), fill_value).otherwise(
        column_name_to_update
    )


def reformat_calibration_df_simple(df: DataFrame, population_column: str, groupby_columns: List[str]):
    """
    Format a dataframe containing multiple population groups and a column of population values
    into a 2xn dataframe of groups and population totals
    Parameters
    ---------
    df
    population_column
    groupby_columns
    """
    for i, col in enumerate(groupby_columns):
        temp_df = (
            df.groupBy(col)
            .agg({population_column: "sum"})
            .withColumnRenamed(col, "group")
            .withColumn(
                "group",
                F.when(
                    F.col("group").isNotNull(),
                    F.concat_ws("", F.lit(f"P{col.split('_')[0][1:]}"), F.col("group").cast("integer")),
                ).otherwise("missing"),
            )
        )
        if i == 0:
            grouped_df = temp_df
        else:
            grouped_df = grouped_df.unionByName(temp_df)

    grouped_df = grouped_df.withColumnRenamed(f"sum({population_column})", "population_total")
    return grouped_df.filter(F.col("group") != "missing")


numeric_column_pattern_map = {
    r"^losa_\d{1,}": "lower_super_output_area_code_{}",  # noqa:W605
    r"^lsoa\d{1,}": "lower_super_output_area_code_{}",  # noqa:W605
    r"^CIS\d{1,}CD": "cis_area_code_{}",  # noqa:W605
    r"^cis\d{1,}cd": "cis_area_code_{}",  # noqa:W605
}


aps_value_map = {
    "country_name": {
        1: "England",
        2: "Wales",
        3: "Scotland",
        4: "Scotland",
        5: "Northen Ireland",
    },
    "ethnicity_aps_england_wales_scotland": {
        1: "white british",
        2: "white irish",
        3: "other white",
        4: "mixed/multiple ethnic groups",
        5: "indian",
        6: "pakistani",
        7: "bangladeshi",
        8: "chinese",
        9: "any other asian  background",
        10: "black/ african /caribbean /black/ british",
        11: "other ethnic group",
    },
    "ethnicity_aps_northen_ireland": {
        1: "white",
        2: "irish traveller",
        3: "mixed/multiple ethnic groups",
        4: "asian/asian british",
        5: "black/ african/ caribbean /black british",
        6: "chinese",
        7: "arab",
        8: "other ethnic group",
    },
}
# fmt: on


def recode_column_values(df: DataFrame, lookup: dict):
    """wrapper to loop over multiple value maps for different columns"""
    for column_name, map in lookup.items():
        df = update_column_values_from_map(df, column_name, map)
    return df


def update_population_values(df: DataFrame):
    maps = {
        "interim_region_code": {
            "E12000001": 1,
            "E12000002": 2,
            "E12000003": 3,
            "E12000004": 4,
            "E12000005": 5,
            "E12000006": 6,
            "E12000007": 7,
            "E12000008": 8,
            "E12000009": 9,
            "W99999999": 10,
            "S99999999": 11,
            "N99999999": 12,
        },
        "interim_sex": {"m": 1, "f": 2},
    }
    age_maps = {
        "age_group_swab": {
            2: 1,
            12: 2,
            17: 3,
            25: 4,
            35: 5,
            50: 6,
            70: 7,
        },
        "age_group_antibodies": {
            16: 1,
            25: 2,
            35: 3,
            50: 4,
            70: 5,
        },
    }
    df = df.withColumn("interim_region_code", F.col("region_code"))
    df = df.withColumn("interim_sex", F.col("sex"))

    df = recode_column_values(df, maps)

    for col, map in age_maps.items():  # type: ignore
        df = assign_named_buckets(df=df, reference_column="age", column_name_to_assign=col, map=map)
    return df


def update_data(df: DataFrame, auxillary_dfs: dict):
    """
    wrapper function for calling update column
    """
    df = update_column(
        df=df,
        lookup_df=auxillary_dfs["postcode_lookup"],
        column_name_to_update="lower_super_output_area_code_11",
        join_on_columns=["country_code_12", "postcode"],
    )
    df = update_column(
        df=df,
        lookup_df=auxillary_dfs["cis_phase_lookup"],
        column_name_to_update="cis_area_code_20",
        join_on_columns=["lower_super_output_area_code_11"],
    )
    return df


# 1165
# requires MATCHED_*col
def update_column(df: DataFrame, lookup_df: DataFrame, column_name_to_update: str, join_on_columns: List[str]):
    """
    Assign column (column_name_to_update) new value from lookup dataframe (lookup_df) if the value does not match
    its counterpart in the old dataframe
    """
    lookup_df = lookup_df.withColumnRenamed(column_name_to_update, f"{column_name_to_update}_from_lookup")
    df = df.join(lookup_df, on=[*join_on_columns], how="left")
    df = df.withColumn(
        column_name_to_update,
        F.when(
            F.col(column_name_to_update).isNull(),
            F.when(
                F.col(f"{column_name_to_update}_from_lookup").isNotNull(), F.col(f"{column_name_to_update}_from_lookup")
            ).otherwise(("N/A")),
        ).otherwise(F.col(column_name_to_update)),
    )
    return df.drop(f"{column_name_to_update}_from_lookup")


# 1163
def clean_df(df: DataFrame):
    """
    Edit column values by applying predefined logic
    """
    df = df.withColumn("country_name_12", F.lower(F.col("country_name_12")))
    drop_columns = [col for col in df.columns if "OLD" in col]
    return df.drop(*drop_columns)


# 1174
def reformat_age_population_table(df: DataFrame, m_f_columns: List[str]):
    """
    recast columns to rows within a population column
    and aggregate the names of these rows into m and f values for sex
    """
    for col in m_f_columns:
        df = df.withColumn(col, F.array(F.lit(col[1:]), F.col(col)))
    cols = [col for col in df.columns if col not in m_f_columns]
    dfs = []
    for sex in ["m", "f"]:
        selected_columns = [col for col in m_f_columns if col[0] == sex]
        dfs.append(
            df.select(*cols, F.explode(F.array(*[F.col(c) for c in selected_columns])).alias("age_population"))
            .withColumn("sex", F.lit(sex))
            .withColumn("age", F.col("age_population")[0].cast("integer"))
            .withColumn("population", F.col("age_population")[1].cast("integer"))
            .drop("age_population")
        )
    return dfs[0].unionByName(dfs[1])
