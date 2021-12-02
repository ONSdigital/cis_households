import re
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from cishouseholds.derive import assign_named_buckets
from cishouseholds.edit import update_column_values_from_map
from cishouseholds.pyspark_utils import get_or_create_spark_session
from cishouseholds.weights.derive import assign_ethnicity_white
from cishouseholds.weights.derive import assign_white_proportion
from cishouseholds.weights.edit import reformat_age_population_table
from cishouseholds.weights.extract import load_auxillary_data


def proccess_population_projection_df(month: int):
    """
    process and format population projections tables by reshaping new datafrmae and recalculating predicted values
    """
    dfs = load_auxillary_data(specify=["population_projection_current", "population_projection_previous", "aps_lookup"])
    previous_projection_df = dfs["population_projection_previous"]
    current_projection_df = dfs["population_projection_current"]
    r = re.compile(r"\w{1}\d{1,}")
    m_f_columns = [
        item for item in list(filter(r.match, current_projection_df.columns)) if item in previous_projection_df.columns
    ]

    selected_columns = [
        "local_authority_unitary_authority_code",
        "region_code",
        "country_code_#",
        "country_name_#",
        *m_f_columns,
    ]
    previous_projection_df = previous_projection_df.select(*selected_columns).withColumn(
        "id", F.monotonically_increasing_id()
    )
    current_projection_df = current_projection_df.select(*selected_columns).withColumn(
        "id", F.monotonically_increasing_id()
    )

    for col in m_f_columns:
        current_projection_df = current_projection_df.withColumnRenamed(col, f"{col}_new")
    current_projection_df = current_projection_df.join(
        previous_projection_df.select("id", *m_f_columns), on="id", how="left"
    )

    if month < 6:
        a = 6 - month
        b = 6 + month
    else:
        a = 18 - month
        b = month - 6

    for col in m_f_columns:
        current_projection_df = current_projection_df.withColumn(
            col, F.lit(1 / 12) * ((a * F.col(col)) + (b * F.col(f"{col}_new")))
        )
        current_projection_df = current_projection_df.drop(f"{col}_new")

    current_projection_df = reformat_age_population_table(current_projection_df, m_f_columns)

    aps_lookup_df = assign_ethnicity_white(
        dfs["aps_lookup"],
        "ethnicity_white",
        "country_name",
        "ethnicity_aps_northen_ireland",
        "ethnicity_aps_engl_wales_scot",
    )
    aps_lookup_df = assign_white_proportion(
        aps_lookup_df,
        "percentage_white_ethnicity_country_over16",
        "ethnicity_white",
        "country_name",
        "person_level_weight_aps_18",
        "age",
    )

    current_projection_df = current_projection_df.join(
        aps_lookup_df.select("country_name", "percentage_white_ethnicity_country_over16"),
        current_projection_df["country_name_#"] == dfs["aps_lookup"]["country_name"],
        how="left",
    )
    current_projection_df = update_values(current_projection_df)
    current_projection_df = calculate_population_totals(
        df=current_projection_df,
        group_by_column="country_name_#",
        population_column="population",
        white_proportion_column="percentage_white_ethnicity_country_over16",
    )
    current_projection_df.toPandas().to_csv("test_output_5.csv", index=False)

    england_df, wales_df, scotland_df, ni_df, england_28_df = get_calibration_dfs(
        current_projection_df, "country_name_#", "age"
    )
    for i, df in enumerate([england_28_df, england_df, wales_df, ni_df, scotland_df]):
        df.toPandas().to_csv(f"country_df{i}.csv", index=False)


def update_values(df: DataFrame):
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

    for col, map in maps.items():
        df = update_column_values_from_map(df, col, map)

    for col, map in age_maps.items():  # type: ignore
        df = assign_named_buckets(df=df, reference_column="age", column_name_to_assign=col, map=map)

    df = df.withColumn(
        "p1_for_swab_longcovid",
        F.when(
            F.col("country_name_#") == "England",
            (F.col("interim_region_code") - 1) * 14 + (F.col("interim_sex") - 1) * 7 + F.col("age_group_swab"),
        ).otherwise((F.col("interim_sex") - 1) * 7 + F.col("age_group_swab")),
    )
    df = df.withColumn(
        "p1_for_antibodies_evernever_engl",
        F.when(
            F.col("country_name_#") == "England",
            ((F.col("interim_region_code") - 1) * 10)
            + ((F.col("interim_sex") - 1) * 5)
            + F.col("age_group_antibodies"),
        ).otherwise(None),
    )
    df = df.withColumn(
        "p1_for_antibodies_28daysto_engl",
        F.when(
            F.col("country_name_#") == "England",
            (F.col("interim_sex") - 1) * 5 + F.col("age_group_antibodies"),
        ).otherwise(None),
    )
    df = df.withColumn(
        "p1_for_antibodies_wales_scot_ni",
        F.when(F.col("country_name_#") == "England", None).otherwise(
            (F.col("interim_sex") - 1) * 5 + F.col("age_group_antibodies"),
        ),
    )
    df = df.withColumn(
        "p3_for_antibodies_28daysto_engl",
        F.when(
            (F.col("country_name_#") == "England") & (F.col("age_group_antibodies").isNull()),
            F.col("interim_region_code"),
        )
        .when(F.col("country_name_#") == "England", "missing")
        .otherwise(None),
    )
    return df


def calculate_population_totals(
    df: DataFrame, group_by_column: str, population_column: str, white_proportion_column: str
):
    window = Window.partitionBy(group_by_column)
    df = df.withColumn(
        "population_country_swab",
        F.sum(F.when(F.col(population_column) >= 2, F.col(population_column)).otherwise(0)).over(window),
    )
    df = df.withColumn(
        "population_country_antibodies",
        F.sum(F.when(F.col(population_column) >= 2, F.col(population_column)).otherwise(0)).over(window),
    )
    df = df.withColumn(
        "p22_white_population_antibodies",
        F.col("population_country_antibodies") * F.col(white_proportion_column),
    )
    return df


def get_calibration_dfs(df: DataFrame, country_column: str, age_column: str):
    def calibarate_df(
        df: DataFrame, groupby_columns: List[str], country: str, min_age: int, additional_columns: List[str] = []
    ):
        df = df.filter(
            (F.col(country_column) != country)
            & (F.col(age_column) >= min_age)
            & (F.col("p1_for_swab_longcovid").isNotNull())
        ).select(*groupby_columns, "population", *additional_columns)
        dfs = []
        for col in groupby_columns:
            reformatted_df = df.groupBy(col).pivot(col).agg({"population": "sum"}).drop(col)
            for p_column in reformatted_df.columns:
                new_column_name = f"P{p_column.rstrip('.0')}"
                reformatted_df = reformatted_df.withColumnRenamed(p_column, new_column_name)
                reformatted_df = reformatted_df.withColumn(
                    new_column_name, F.lit(reformatted_df.agg({new_column_name: "max"}).collect()[0][0])
                )
            expr = [F.last(col).alias(col) for col in reformatted_df.columns]
            reformatted_df = reformatted_df.agg(*expr)
            dfs.append(reformatted_df)

        if len(dfs) == 1:
            return dfs[0]
        else:
            spark = get_or_create_spark_session()
            spark.conf.set("spark.sql.crossJoin.enabled", "true")
            return_df = dfs[0].withColumn("TEMP", F.lit(1))
            for df in dfs[1:]:
                df = df.withColumn("TEMP", F.lit(1))
                return_df = return_df.join(df, return_df.TEMP == df.TEMP.alias("DF_TEMP"), how="inner").drop("TEMP")
            return return_df

    england_df = calibarate_df(df=df, groupby_columns=["p1_for_swab_longcovid"], country="England", min_age=2)
    wales_df = calibarate_df(
        df=df,
        groupby_columns=["p1_for_antibodies_wales_scot_ni"],
        country="Wales",
        min_age=16,
        additional_columns=["p22_white_population_antibodies"],
    )
    scotland_df = calibarate_df(
        df=df, groupby_columns=["p1_for_antibodies_wales_scot_ni"], min_age=16, country="Scotland"
    )
    ni_df = calibarate_df(
        df=df, groupby_columns=["p1_for_antibodies_wales_scot_ni"], min_age=16, country="Northern Ireland"
    )

    england_28_df = calibarate_df(
        df=df,
        groupby_columns=["p1_for_antibodies_28daysto_engl", "p3_for_antibodies_28daysto_engl"],
        country="England",
        min_age=16,
        additional_columns=["p22_white_population_antibodies"],
    )

    return england_df, wales_df, scotland_df, ni_df, england_28_df


proccess_population_projection_df(
    month=7,
)
