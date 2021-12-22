from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

from cishouseholds.derive import assign_from_lookup
from cishouseholds.derive import assign_named_buckets


# 1178
def survey_extraction_household_data_response_factor(
    df: DataFrame,
    # hh_samples_df: DataFrame,
    df_extract_by_country: DataFrame,
    # table_name: str,
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
    # STEP 1 - create: extract_dataset as per requirements - TODO can we test this?
    # (i.e extract dataset for calibration from survey_data dataset (individual level))
    # df = extract_from_table(table_name).select(*required_extracts_column_list) TODO temporarily disabled
    # will be added once the mock pytest is enabled

    # STEP 2 -  check there are no missing values
    df = df.withColumn("check_if_missing", F.lit(None))
    for var in required_extracts_column_list:
        df = df.withColumn(
            "check_if_missing", F.when(F.col(var).isNull(), 1).otherwise(F.col("check_if_missing"))
        )  # TODO: check if multiple columns are needed

    # STEP 3 -  create household level of the extract to be used in calculation of non response factor
    df = (
        df.withColumn("response_indicator", F.when(F.col("participant_id").isNotNull(), 1).otherwise(None))
        # .drop("participant_id") # ?
        # .dropDuplicates("ons_household_id") # ?
    )

    # df = df.join(
    #     hh_samples_df,
    #     on='participant_id',
    #     how="left"
    # )

    # STEP 4 - merge hh_samples_df (36 processing step) and household level extract (69 processing step)
    # TODO: check if population_by_country comes by country separated or together
    df_extract_by_country = df_extract_by_country.withColumnRenamed("country_name_12", "country_name_12_right")
    df = df.join(
        df_extract_by_country,
        (
            (df["country_name_12"] == df_extract_by_country["country_name_12_right"])
            & ((df["antibodies"] == 1) | ((df["swab"] == 1) | (df["longcovid"] == 1)))
        ),
        how="left",
    ).drop("country_name_12" + "_right")

    # TODO: is there a possibility that population_country_swab and population_country_antibodies values
    # are kept in the same record?
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
            F.col("response_indicator") == 1,  # TODO: consider interim_participant_id as well
            F.round(F.col("household_level_designweight_swab") * F.col("bounded_non_response_factor"), 1),
        ),
    )

    df = df.withColumn(
        "household_level_designweight_adjusted_antibodies",
        F.when(
            F.col("response_indicator") == 1,
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
    # TODO: use validate_design_weights() stefen's function
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

    # check_4: if they are the same across cis_area_code_20 by sample groups (by sample_source)
    # TODO: testdata - create a column done for sampling then filter out to extract the singular samplings.
    # These should have the same dweights when window function is applied.
    check_4 = True
    return check_1, check_2_3, check_4


# 1180
def grouping_from_lookup(df):
    """
    Parameters
    ----------
    df
    """
    # A.1 re-code the region_code values, by replacing the alphanumeric code with numbers from 1 to 12
    spark = SparkSession.builder.getOrCreate()
    region_code_lookup_df = spark.createDataFrame(
        data=[
            ("E12000001", 1),
            ("E12000002", 2),
            ("E12000003", 3),
            ("E12000004", 4),
            ("E12000005", 5),
            ("E12000006", 6),
            ("E12000007", 7),
            ("E12000008", 8),
            ("E12000009", 9),
            ("W99999999", 10),
            ("S99999999", 11),
            ("N99999999", 12),
        ],
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
        data=[
            ("male", 1),
            ("female", 2),
        ],
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
    map_age_at_visit_swab = {2: 1, 12: 2, 17: 3, 25: 4, 35: 5, 50: 6, 70: 7}
    df = assign_named_buckets(
        df=df,
        column_name_to_assign="age_group_swab",
        reference_column="age_at_visit",
        map=map_age_at_visit_swab,
    )

    # A.4 create age groups considering certain age boundaries,
    # needed for calibration weighting of antibodies data
    map_age_at_visit_antibodies = {16: 1, 25: 2, 35: 3, 50: 4, 70: 5}
    df = assign_named_buckets(
        df=df,
        column_name_to_assign="age_group_antibodies",
        reference_column="age_at_visit",
        map=map_age_at_visit_antibodies,
    )

    # A.5 Creating first partition(p1)/calibration variable
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
    calibration_dic = {
        "p1_swab_longcovid_england": {
            "dataset": [
                "swab_evernever",
                "swab_14days",
                "longcovid_24days",
                "longcovid_42days",
            ],
            "condition": ((F.col("country_name_12") == "england"))
            & (
                (
                    (F.col("swab") == 1) & (F.col("ever_never") == 1) | (F.col("14_days") == 1)
                )  # TODO: is it OR(ever_never, 14_days)?
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
                | (F.col("country_name_12") == "northern_ireland")  # TODO: double-check name
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
                | (F.col("country_name_12") == "northern_ireland")  # TODO: double-check name
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
                & (F.col("age_at_visit") >= 16)  # TODO: age of visit to be put as input?
                & (F.col("antibodies") == 1)
                & (F.col("28_days") == 1)
            ),
            "operation": (F.col("interim_region_code")),
        },
    }
    # TODO-QUESTION: are the dataframes organised by country so that column country isnt needed?

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

    for calibration_type in calibration_dic.keys():
        df = df.withColumn(
            calibration_type,
            F.when(
                calibration_dic[calibration_type]["condition"],
                calibration_dic[calibration_type]["operation"],
            ),
        )
        for column_dataset in calibration_dic[calibration_type]["dataset"]:
            df = df.withColumn(
                column_dataset,
                F.when(calibration_dic[calibration_type]["condition"], F.lit(1)).otherwise(F.col(column_dataset)),
            )
    return df


# 1180
def generate_datasets_to_be_weighted_for_calibration(
    df: DataFrame,
    processing_step: int
    # dataset,
    # dataset_type:str
):
    """
    Parameters
    ----------
    df
    processing_step:
        1 for
            england_swab_evernever, england_swab_14days, england_longcovid_28days, england_longcovid_42days
        2 for
            wales_swab_evernever, wales_swab_14days, wales_longcovid_28days, wales_longcovid_42days,
            scotland_swab_evernever, scotland_swab_14days, scotland_longcovid_28days, scotland_longcovid_42days,
            northen_ireland_swab_evernever, northen_ireland_swab_14days, northen_ireland_longcovid_28days,
            northen_ireland_longcovid_42days
        3 for
            england_antibodies_evernever
        4 for
            england_antibodies_28daysto
        5 for
            wales_antibodies_evernever
            wales_antibodies_28daysto
        6 for
            scotland_antibodies_evernever
            scotland_antibodies_28daysto
            northen_ireland_antibodies_evernever
            northen_ireland_antibodies_28daysto
    """
    dataset_dict = {
        1: {
            "variable": ["england"],
            "keep_var": [
                "country_name_12",
                "participant_id",
                "scaled_design_weight_adjusted_swab",
                "p1_swab_longcovid_england",
            ],
            "create_dataset": [
                "england_swab_evernever",
                "england_swab_14days",
                "england_longcovid_24days",
                "england_longcovid_42days",
            ],
        },
        2: {
            "variable": ["wales", "scotland", "northen_ireland"],
            "keep_var": [
                "country_name_12",
                "participant_id",
                "scaled_design_weight_adjusted_swab",
                "p1_swab_longcovid_wales_scot_ni",
            ],
            "create_dataset": [
                "wales_swab_evernever",
                "wales_swab_14days",
                "wales_longcovid_24days",
                "wales_longcovid_42days",
                "scotland_swab_evernever",
                "scotland_swab_14days",
                "scotland_longcovid_24days",
                "scotland_longcovid_42days",
                "northen_ireland_swab_evernever",
                "northen_ireland_swab_14days",
                "northen_ireland_longcovid_24days",
                "northen_ireland_longcovid_42days",
            ],
        },
        3: {
            "variable": ["england"],
            "keep_var": [
                "country_name_12",
                "participant_id",
                "scaled_design_weight_adjusted_antibodies",
                "p1_for_antibodies_evernever_engl",
                "p2_for_antibodies",
            ],
            "create_dataset": ["england_antibodies_evernever"],
        },
        4: {
            "variable": ["england"],
            "keep_var": [
                "country_name_12",
                "participant_id",
                "scaled_design_weight_adjusted_antibodies",
                "p1_for_antibodies_28daysto_engl",
                "p2_for_antibodies",
                "p3_for_antibodies_28daysto_engl",
            ],
            "create_dataset": ["england_antibodies_28daysto"],
        },
        5: {
            "variable": ["wales"],
            "keep_var": [
                "country_name_12",
                "participant_id",
                "scaled_design_weight_adjusted_antibodies",
                "p1_for_antibodies_wales_scot_ni",
                "p2_for_antibodies",
            ],
            "create_dataset": ["wales_antibodies_evernever", "wales_antibodies_28daysto"],
        },
        6: {
            "variable": ["scotland", "northen_ireland"],
            "keep_var": [
                "country_name_12",
                "participant_id",
                "scaled_design_weight_adjusted_antibodies",
                "p1_for_antibodies_wales_scot_ni",
            ],
            "create_dataset": [
                "scotland_antibodies_evernever",
                "scotland_antibodies_28daysto",
                "northen_ireland_antibodies_evernever",
                "northen_ireland_antibodies_28daysto",
            ],
        },
    }

    df = df.where(F.col("country_name_12").isin(dataset_dict[processing_step]["variable"])).select(
        *dataset_dict[processing_step]["keep_var"]
    )

    # df.where(F.col('country_name_12').isin(dataset_dict[processing_step]['variable']))
    # TODO: create datasets dataset_dict[processing_step]['create_dataset']

    # TODO: no need to create multiple df
    return df
