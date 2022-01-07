from typing import List

import yaml
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

from cishouseholds.derive import assign_from_lookup
from cishouseholds.derive import assign_named_buckets


def pre_calibration_high_level(df: DataFrame, df_country: DataFrame) -> DataFrame:
    """
    Parameters
    ----------
    df
    df_country
    """
    df = survey_extraction_household_data_response_factor(
        df=df,
        df_extract_by_country=df_country,
        required_extracts_column_list=["ons_household_id", "participant_id", "sex", "ethnicity_white", "age_at_visit"],
    )
    df = derive_index_multiple_deprivation_group(df)
    df = derive_total_responded_and_sampled_households(df)
    df = calculate_non_response_factors(df, n_decimals=3)
    df = adjust_design_weight_by_non_response_factor(df)
    df = adjusted_design_weights_to_population_totals(df)
    df = grouping_from_lookup(df)
    df = create_calibration_var(df)

    return df


# 1178
def survey_extraction_household_data_response_factor(
    df: DataFrame,
    df_extract_by_country: DataFrame,
    required_extracts_column_list: List[str],
) -> DataFrame:
    """
    Parameters
    ----------
    hh_samples_df
    df_extract_by_country
    table_name
    required_extracts_column_list
    mandatory_extracts_column_list
    population_join_column: Only swab/antibodies

    """
    # STEP 1 - create: extract_dataset as per requirements -
    # (i.e extract dataset for calibration from survey_data dataset (individual level))
    # will be added once the mock pytest is enabled

    # STEP 2 -  check there are no missing values
    df = df.withColumn("check_if_missing", F.lit(None))
    for var in required_extracts_column_list:
        df = df.withColumn("check_if_missing", F.when(F.col(var).isNull(), 1).otherwise(F.col("check_if_missing")))

    # STEP 3 -  create household level of the extract to be used in calculation of non response factor
    df = (
        df.withColumn("response_indicator", F.when(F.col("participant_id").isNotNull(), 1).otherwise(None))
        # .drop("participant_id") # ?
        # .dropDuplicates("ons_household_id") # ?
    )

    # STEP 4 - merge hh_samples_df (36 processing step) and household level extract (69 processing step)
    df_extract_by_country = df_extract_by_country.withColumnRenamed("country_name_12", "country_name_12_right")
    df = df.join(
        df_extract_by_country,
        (
            (df["country_name_12"] == df_extract_by_country["country_name_12_right"])
            & ((df["antibodies"] == 1) | ((df["swab"] == 1) | (df["longcovid"] == 1)))
        ),
        how="left",
    ).drop("country_name_12" + "_right")

    df = df.withColumn(
        "population_country_swab",
        F.when((F.col("swab") == 1) | (F.col("longcovid") == 1), F.col("population_country_swab")).otherwise(None),
    )
    df = df.withColumn(
        "population_country_antibodies",
        F.when((F.col("antibodies") == 1), F.col("population_country_antibodies")).otherwise(None),
    )
    return df


# 1179 part1
def derive_index_multiple_deprivation_group(df: DataFrame):
    """
    Parameters
    ----------
    df
    """
    # A.1 group households  considering  values to index of multiple deprivation
    map_index_multiple_deprivation_group_country = {
        "england": {0: 1, 6570: 2, 13139: 3, 19708: 4, 26277: 5},
        "wales": {0: 1, 383: 2, 765: 3, 1147: 4, 1529: 5},
        "scotland": {0: 1, 1396: 2, 2791: 3, 4186: 4, 5581: 5},
        "northern_ireland": {0: 1, 179: 2, 357: 3, 535: 4, 713: 5},
    }

    for country in map_index_multiple_deprivation_group_country.keys():
        df = assign_named_buckets(
            df=df,
            reference_column="index_multiple_deprivation",
            column_name_to_assign=f"index_multiple_deprivation_group_{country}",
            map=map_index_multiple_deprivation_group_country[country],
        )

    df = df.withColumn(
        "index_multiple_deprivation_group",
        (
            F.when(F.lower(F.col("country_name_12")) == "wales", F.col("index_multiple_deprivation_group_wales"))
            .when(F.lower(F.col("country_name_12")) == "england", F.col("index_multiple_deprivation_group_england"))
            .when(F.lower(F.col("country_name_12")) == "scotland", F.col("index_multiple_deprivation_group_scotland"))
            .when(
                F.lower(F.col("country_name_12")) == "northern ireland",
                F.col("index_multiple_deprivation_group_northern_ireland"),
            )
        ),
    )

    for country in map_index_multiple_deprivation_group_country.keys():
        df = df.drop(f"index_multiple_deprivation_group_{country}")

    return df


# 1179 part2
def derive_total_responded_and_sampled_households(df: DataFrame) -> DataFrame:
    """
    A.2/3 create a variable to indicate for each country, how many households are in the sample,
          create a variable to indicate for each country, how many households participated to the survey
    Parameters
    ----------
    df
    country_column: For northen_ireland, use country, otherwise use cis_area_code_20
    """

    df = df.withColumn("country_name_lower", F.lower(F.col("country_name_12")))

    window_list_nni = ["sample_addressbase_indicator", "cis_area_code_20", "index_multiple_deprivation_group"]
    window_list_ni = ["sample_addressbase_indicator", "country_name_lower", "index_multiple_deprivation_group"]

    w1_nni = Window.partitionBy(*window_list_nni)
    w1_ni = Window.partitionBy(*window_list_ni)

    df = df.withColumn(
        "total_sampled_households_cis_imd_addressbase",
        F.when(
            F.lower(F.col("country_name_12")) != "northern ireland",
            F.count(F.col("ons_household_id")).over(w1_nni).cast("int"),
        ).otherwise(F.count(F.col("ons_household_id")).over(w1_ni).cast("int")),
    )

    w2_nni = Window.partitionBy(*window_list_nni, "interim_participant_id")
    w2_ni = Window.partitionBy(*window_list_ni, "interim_participant_id")
    df = df.withColumn(
        "total_responded_households_cis_imd_addressbase",
        F.when(
            F.lower(F.col("country_name_12")) != "northern ireland",
            F.count(F.col("ons_household_id")).over(w2_nni).cast("int"),
        ).otherwise(F.count(F.col("ons_household_id")).over(w2_ni).cast("int")),
    )

    df = df.withColumn(
        "total_responded_households_cis_imd_addressbase",
        F.when(F.col("interim_participant_id") != 1, 0).otherwise(
            F.col("total_responded_households_cis_imd_addressbase")
        ),
    )

    df = df.withColumn(
        "auxiliary",
        F.when(
            F.lower(F.col("country_name_12")) != "northern ireland",
            F.max(F.col("total_responded_households_cis_imd_addressbase")).over(w1_nni),
        ).otherwise(F.max(F.col("total_responded_households_cis_imd_addressbase")).over(w1_ni)),
    )

    df = df.withColumn("total_responded_households_cis_imd_addressbase", F.col("auxiliary")).drop(
        "auxiliary", "country_name_lower"
    )
    return df


# 1179 part 3
def calculate_non_response_factors(df: DataFrame, n_decimals: int = 2) -> DataFrame:
    """
    B.1 calculate raw non_response_factor by dividing total_sampled_households_cis_imd_addressbase
        total_responded_households_cis_imd_addressbase (derived in previous steps)
    B.2 calculate scaled non_response_factor by dividing raw_non_response_factor to the mean
        of raw_non_response_factor
    B.3 calculate bounded non_response_factor by adjusting the values of the scaled non_response
        factor to be contained in a certain, pre-defined range
    Parameters
    ----------
    df
    """
    df = df.withColumn(
        "raw_non_response_factor",
        F.round(
            F.col("total_sampled_households_cis_imd_addressbase")
            / F.col("total_responded_households_cis_imd_addressbase"),
            1,
        ).cast("double"),
    )

    w_country = Window.partitionBy("country_name_12")

    df = df.withColumn(
        "mean_raw_non_response_factor",
        F.round(F.mean(F.col("raw_non_response_factor")).over(w_country), n_decimals),
    )

    df = df.withColumn(
        "scaled_non_response_factor",
        F.round(F.col("raw_non_response_factor") / F.col("mean_raw_non_response_factor"), n_decimals),
    )

    df = df.withColumn(
        "bounded_non_response_factor",
        F.when(F.col("scaled_non_response_factor") < 0.5, 0.6)
        .when(F.col("scaled_non_response_factor") > 2.0, 1.8)
        .otherwise(None),
    )

    return df


# 1179 Part 4
def adjust_design_weight_by_non_response_factor(df: DataFrame) -> DataFrame:
    """
    C1. adjust design weights by the non_response_factor by multiplying the design
    weights with the non_response factor
    """

    df = df.withColumn(
        "household_level_designweight_adjusted_swab",
        F.when(
            (F.col("response_indicator") == 1) & ((F.col("longcovid") == 1) | (F.col("swab") == 1)),
            F.round(F.col("household_level_designweight_swab") * F.col("bounded_non_response_factor"), 1),
        ),
    )

    df = df.withColumn(
        "household_level_designweight_adjusted_antibodies",
        F.when(
            (F.col("response_indicator") == 1) & (F.col("antibodies") == 1),
            F.round(F.col("household_level_designweight_antibodies") * F.col("bounded_non_response_factor"), 1),
        ),
    )

    return df


# 1179 Part 5
def adjusted_design_weights_to_population_totals(df: DataFrame) -> DataFrame:
    """
    D.1 calculate the scaling factor and then apply the scaling factor the
    design weights adjusted by the non response factor.
    If there is a swab or long covid type dataset, then use the population
    totlas for swab and the naming will have "swab" incorporated.
    Same for antibodies type dataset
    sum_adjusted_design_weight_swab or antibodies
    """
    w_country = Window.partitionBy("country_name_12")

    test_type_list = ["antibodies", "swab"]

    for test_type in test_type_list:
        df = df.withColumn(
            "sum_adjusted_design_weight_" + test_type,
            F.when(
                F.col("response_indicator") == 1,
                F.round(F.sum(F.col(f"household_level_designweight_adjusted_{test_type}")).over(w_country), 1),
            ),
        )

        df = df.withColumn(
            "scaling_factor_adjusted_design_weight_" + test_type,
            F.when(
                F.col("response_indicator") == 1,
                F.round(F.col(f"population_country_{test_type}") / F.col(f"sum_adjusted_design_weight_{test_type}"), 1),
            ),
        )

        # scaled_design_weight_adjusted_swab or antibodies
        df = df.withColumn(
            "scaled_design_weight_adjusted_" + test_type,
            F.when(
                F.col("response_indicator") == 1,
                F.round(
                    F.col(f"scaling_factor_adjusted_design_weight_{test_type}")
                    * F.col(f"household_level_designweight_adjusted_{test_type}"),
                    1,
                ),
            ),
        )

    return df


# 1179
def precalibration_checkpoints(df: DataFrame, test_type: str, dweight_list: List[str]) -> DataFrame:
    """
    Parameters
    ----------
    df
    test_type
    population_totals
    dweight_list
    """
    # check_1: The design weights are adding up to total population
    check_1 = (
        df.select(F.sum(F.col("scaled_design_weight_adjusted_" + test_type))).collect()[0][0]
        == df.select("number_of_households_population_by_cis").collect()[0][0]
    )
    # check_2 and check_3: The  design weights are all are positive AND check there are no missing design weights
    df = df.withColumn("not_positive_or_null", F.lit(None))

    for dweight in dweight_list:
        df = df.withColumn(
            "not_positive_or_null",
            F.when((F.col(dweight) < 0) | (F.col(dweight).isNull()), 1).otherwise(F.col("not_positive_or_null")),
        )

    check_2_3 = 0 == len(
        df.distinct().where(F.col("not_positive_or_null") == 1).select("not_positive_or_null").collect()
    )

    # TODO check_4: if they are the same across cis_area_code_20 by sample groups (by sample_source)
    check_4 = True
    return check_1, check_2_3, check_4


# 1180 - TODO: make them external
def grouping_from_lookup(df):
    """
    Parameters
    ----------
    df
    """
    # A.1 re-code the region_code values, by replacing the alphanumeric code with numbers from 1 to 12
    spark = SparkSession.builder.getOrCreate()

    lookup_dict = yaml.safe_load(open("cishouseholds/weights/precal_config.yaml", "r"))

    region_code_lookup_df = spark.createDataFrame(
        data=[(k, v) for k, v in lookup_dict["region_code"].items()],
        schema="region_code string, interim_region_code integer",
    )
    df = assign_from_lookup(
        df=df,
        column_name_to_assign="interim_region_code",
        reference_columns=["region_code"],
        lookup_df=region_code_lookup_df,
    )

    # A.2 re-code sex variable replacing string with integers
    sex_code_lookup_df = spark.createDataFrame(
        data=[(k, v) for k, v in lookup_dict["sex_code"].items()],
        schema="sex string, interim_sex integer",
    )
    df = assign_from_lookup(
        df=df,
        column_name_to_assign="interim_sex",
        reference_columns=["sex"],
        lookup_df=sex_code_lookup_df,
    )

    # A.3 create age groups considering certain age boundaries,
    # as needed for calibration weighting of swab data
    df = assign_named_buckets(
        df=df,
        column_name_to_assign="age_group_swab",
        reference_column="age_at_visit",
        map=lookup_dict["age_at_visit_swab"],
    )

    # A.4 create age groups considering certain age boundaries,
    # needed for calibration weighting of antibodies data
    df = assign_named_buckets(
        df=df,
        column_name_to_assign="age_group_antibodies",
        reference_column="age_at_visit",
        map=lookup_dict["age_at_visit_antibodies"],
    )
    return df


# 1180
def create_calibration_var(
    df: DataFrame,
) -> DataFrame:
    """
    Parameters
    ----------
    df
    dataset
    calibration_type:
        allowed values:
            p1_swab_longcovid_england
            p1_swab_longcovid_wales_scot_ni
            p1_for_antibodies_evernever_engl
            p1_for_antibodies_28daysto_engl
            p1_for_antibodies_wales_scot_ni
            p2_for_antibodies
            p3_for_antibodies_28daysto_engl
    dataset_type - allowed values:
        swab_evernever
        swab_14days
        longcovid_24days
        longcovid_42days
        longcovid_24days
        antibodies_evernever
        antibodies_28daysto
    """
    # A.5 Creating first partition(p1)/calibration variable
    precalibration_dic = {
        "p1_swab_longcovid_england": {
            "dataset": [
                "swab_evernever",
                "swab_14days",
                "longcovid_24days",
                "longcovid_42days",
            ],
            "condition": ((F.col("country_name_12") == "england"))
            & (
                ((F.col("swab") == 1) & ((F.col("ever_never") == 1) | (F.col("7_days") == 1) | (F.col("14_days") == 1)))
                | (
                    (F.col("longcovid") == 1) & ((F.col("28_days") == 1) | (F.col("42_days") == 1))
                )  # assumed to be OR(28, 42_days)
            ),
            "operation": (
                (F.col("interim_region_code") - 1) * 14 + (F.col("interim_sex") - 1) * 7 + F.col("age_group_swab")
            ),
        },
        "p1_swab_longcovid_wales_scot_ni": {
            "dataset": [
                "longcovid_24days",
                "longcovid_42days",
                "swab_evernever",
            ],
            "condition": (
                (F.col("country_name_12") == "wales")
                | (F.col("country_name_12") == "scotland")
                | (F.col("country_name_12") == "northern_ireland")
            )
            & (
                ((F.col("swab") == 1) & (F.col("ever_never") == 1) & (F.col("14_days") == 1))
                | (
                    (F.col("longcovid") == 1) & ((F.col("28_days") == 1) | (F.col("42_days") == 1))
                )  # Assumed OR(28_day, 42_day)
            ),
            "operation": ((F.col("interim_sex") - 1) * 7 + F.col("age_group_swab")),
        },
        "p1_for_antibodies_evernever_engl": {
            "dataset": ["antibodies_evernever"],
            "condition": (F.col("country_name_12") == "england")
            & ((F.col("antibodies") == 1) & (F.col("ever_never") == 1)),  # clarify if OR(antibodies, ever_never)
            "operation": (
                (F.col("interim_region_code") - 1) * 10 + (F.col("interim_sex") - 1) * 5 + F.col("age_group_antibodies")
            ),
        },
        "p1_for_antibodies_28daysto_engl": {
            "dataset": ["antibodies_28daysto"],
            "condition": (F.col("country_name_12") == "england") & (F.col("antibodies") == 1) & (F.col("28_days") == 1),
            "operation": (F.col("interim_sex") - 1) * 5 + F.col("age_group_antibodies"),
        },
        "p1_for_antibodies_wales_scot_ni": {
            "dataset": ["antibodies_evernever", "antibodies_28daysto"],
            "condition": (
                (F.col("country_name_12") == "wales")
                | (F.col("country_name_12") == "scotland")
                | (F.col("country_name_12") == "northern_ireland")
            )
            & ((F.col("antibodies") == 1) & (F.col("ever_never") == 1) & (F.col("28_days") == 1)),
            "operation": ((F.col("interim_sex") - 1) * 5 + F.col("age_group_antibodies")),
        },
        "p2_for_antibodies": {
            "dataset": [
                "antibodies_evernever",
                "antibodies_28daysto",
            ],
            "condition": ((F.col("country_name_12") == "wales") | (F.col("country_name_12") == "england"))
            & (
                ((F.col("antibodies") == 1) & (F.col("ever_never") == 1))
                | ((F.col("antibodies") == 1) & (F.col("28_days") == 1))
            ),
            "operation": (F.col("ethnicity_white") + 1),
        },
        "p3_for_antibodies_28daysto_engl": {
            "dataset": ["antibodies_28daysto"],
            "condition": (
                (F.col("country_name_12") == "england")
                & (F.col("age_at_visit") >= 16)
                & (F.col("antibodies") == 1)
                & (F.col("28_days") == 1)
            ),
            "operation": (F.col("interim_region_code")),
        },
    }

    # A.6 Create second partition (p2)/calibration variable
    dataset_column = [
        "swab_evernever",
        "swab_14days",
        "longcovid_24days",
        "longcovid_42days",
        "antibodies_evernever",
        "antibodies_28daysto",
    ]
    for column in dataset_column:
        df = df.withColumn(column, F.lit(None))

    for calibration_type in precalibration_dic.keys():
        df = df.withColumn(
            calibration_type,
            F.when(
                precalibration_dic[calibration_type]["condition"],
                precalibration_dic[calibration_type]["operation"],
            ),
        )
        for column_dataset in precalibration_dic[calibration_type]["dataset"]:
            df = df.withColumn(
                column_dataset,
                F.when(precalibration_dic[calibration_type]["condition"], F.lit(1)).otherwise(F.col(column_dataset)),
            )
    return df
