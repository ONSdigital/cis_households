from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def null_to_value(df: DataFrame, column_name_to_update: str, value: int = 0):
    return df.withColumn(
        column_name_to_update,
        F.when((F.col(column_name_to_update).isNull()) | (F.isnan(F.col(column_name_to_update))), value).otherwise(
            F.col(column_name_to_update)
        ),
    )


def update_data(df: DataFrame, auxillary_dfs: dict):
    """
    wrapper function for calling update column
    """
    df = update_column(
        df, auxillary_dfs["nspl_lookup"], "lower_super_output_area_code_11", ["country_code_12", "postcode"]
    )
    df = update_column(df, auxillary_dfs["cis20cd_lookup"], "cis_area_code_20", ["lower_super_output_area_code_11"])
    drop_columns = [col for col in df.columns if "MATCHED" in col]
    return df.drop(*drop_columns)


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
            F.col(f"MATCHED_{column_name_to_update}").isNull(),
            F.when(
                F.col(f"{column_name_to_update}_from_lookup").isNotNull(), F.col(f"{column_name_to_update}_from_lookup")
            ).otherwise(("N/A")),
        ).otherwise(F.col(column_name_to_update)),
    )
    return df.drop(f"{column_name_to_update}_from_lookup")


def clean_df(df: DataFrame):
    """
    Edit column values by applying predefined logic
    """
    df = df.withColumn("country_name_12", F.lower(F.col("country_name_12")))
    drop_columns = [col for col in df.columns if "OLD" in col]
    return df.drop(*drop_columns)


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
            .withColumn("population", F.col("age_population")[1])
            .drop("age_population")
        )
    return dfs[0].unionByName(dfs[1])
