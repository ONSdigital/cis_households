from typing import List
from typing import Union

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import SparkSession as spark_session
from pyspark.sql.window import Window

from cishouseholds.derive import assign_from_lookup
from cishouseholds.derive import assign_named_buckets
from cishouseholds.pipeline.load import extract_from_table


# 1167
def chose_scenario_of_dweight_for_antibody_different_household(
    df: DataFrame,
    eligibility_pct: str,
    tranche_eligible_indicator: str,
    # household_samples_dataframe: List[str],
    n_eligible_hh_tranche_bystrata_column,
    n_sampled_hh_tranche_bystrata_column,
) -> Union[str, None]:
    """
    Decide what scenario to use for calculation of the design weights
    for antibodies for different households
    Parameters
    ----------
    df
    eligibility_pct
    tranche_eligible_indicator
    household_samples_dataframe
    n_eligible_hh_tranche_bystrata_column
    n_sampled_hh_tranche_bystrata_column
    """
    df = df.withColumn(
        eligibility_pct,
        F.when(
            F.col(n_eligible_hh_tranche_bystrata_column).isNull()
            & F.col(n_sampled_hh_tranche_bystrata_column).isNull(),
            0,
        )
        .when(
            F.col(n_eligible_hh_tranche_bystrata_column).isNotNull()
            & F.col(n_sampled_hh_tranche_bystrata_column).isNotNull()
            & (F.col(n_sampled_hh_tranche_bystrata_column) > 0),
            (
                100
                * (n_eligible_hh_tranche_bystrata_column - n_sampled_hh_tranche_bystrata_column)
                / n_sampled_hh_tranche_bystrata_column
            ),
        )
        .otherwise(None),  # TODO: check this
    )

    if not tranche_eligible_indicator:  # TODO: not in household_samples_dataframe?
        return "A"
    else:
        if eligibility_pct == 0:
            return "B"
        else:
            return "C"


# 1168
def raw_dweight_for_AB_scenario_for_antibody(
    df: DataFrame,
    hh_dweight_antibodies_column,
    raw_dweight_antibodies_column,
    sample_new_previous_column,
    scaled_dweight_swab_nonadjusted_column,
) -> DataFrame:
    """
    Parameters
    ----------
    df
    hh_dweight_antibodies_column
    raw_dweight_antibodies_column
    sample_new_previous_column
    scaled_dweight_swab_nonadjusted_column
    """
    df = df.withColumn(
        raw_dweight_antibodies_column,
        F.when(F.col(hh_dweight_antibodies_column).isNotNull(), F.col(hh_dweight_antibodies_column)),
    )
    df = df.withColumn(
        raw_dweight_antibodies_column + "_b",  # TODO: should this be AB
        F.when(
            (F.col(sample_new_previous_column) == "previous") & (F.col(hh_dweight_antibodies_column).isNotNull()),
            F.col(hh_dweight_antibodies_column),
        ).when(
            (F.col(sample_new_previous_column) == "new") & (F.col(hh_dweight_antibodies_column).isNull()),
            F.col(scaled_dweight_swab_nonadjusted_column),
        ),
    )
    df = df.withColumn(
        raw_dweight_antibodies_column,
        F.when(hh_dweight_antibodies_column).isNull(),
        F.col(scaled_dweight_swab_nonadjusted_column),
    )
    return df


# 1169
def function_name_1169(
    df: DataFrame,
    sample_new_previous: str,
    tranche_eligible_hh: str,
    tranche_n_indicator: str,
    raw_dweight_antibodies_c: str,
    scaled_dweight_swab_nonadjusted: str,
    tranche_factor: str,
    hh_dweight_antibodies_c: str,
    dweights_swab: str,
) -> DataFrame:
    """
    1 step: for cases with sample_new_previous = "previous" AND tranche_eligible_households=yes(1)
    AND  tranche_number_indicator = max value, calculate raw design weight antibodies by using
    tranche_factor
    2 step: for cases with sample_new_previous = "previous";  tranche_number_indicator != max value;
    tranche_ eligible_households != yes(1), calculate the  raw design weight antibodies by using
    previous dweight antibodies
    3 step: for cases with sample_new_previous = new, calculate  raw design weights antibodies
    by using  design weights for swab

    Parameters
    ----------
    df
    sample_new_previous
    tranche_eligible_hh
    tranche_n_indicator
    raw_dweight_antibodies_c
    scaled_dweight_swab_nonadjusted
    tranche_factor
    hh_dweight_antibodies_c
    dweights_swab
    """
    max_value = df.agg({"tranche": "max"}).first()[0]

    df = df.withColumn(
        "raw_design_weight_antibodies",
        F.when(
            (F.col(sample_new_previous) == "previous")
            & (F.col(tranche_eligible_hh) == "Yes")
            & (F.col(tranche_n_indicator) == max_value),
            F.col(scaled_dweight_swab_nonadjusted) * F.col(tranche_factor),
        )
        .when(
            (F.col(sample_new_previous) == "previous")
            & (F.col(tranche_n_indicator) != max_value)
            & (F.col(tranche_eligible_hh) != "Yes"),
            F.col(hh_dweight_antibodies_c),
        )
        .when(
            (F.col(sample_new_previous) == "new")
            & (F.col(raw_dweight_antibodies_c) == F.col(scaled_dweight_swab_nonadjusted)),
            F.col(dweights_swab),
        ),
    )
    return df


# 1170
def function_name_1170(df: DataFrame, dweights_column: str, carryforward_dweights_antibodies_column: str) -> DataFrame:
    """
    Bring the design weights calculated up until this point  into same variable: carryforward_dweights_antibodies.
    Scaled up to population level  carryforward design weight antibodies.
    Parameters
    ----------
    df
    dweights_column
    carryforward_dweights_antibodies_column
    """
    # part 1: bring the design weights calculated up until this point  into same variable:
    #  carryforward_dweights_antibodies
    df = df.withColumn(carryforward_dweights_antibodies_column, F.col(dweights_column))  # TODO

    # part 2: scaled up to population level  carryforward design weight antibodies

    return df


# 1171
def function_name_1171(
    df: DataFrame,
    dweights_antibodies_column: str,
    total_population_column: str,
    cis_area_code_20_column: str,
) -> DataFrame:
    """
    check the dweights_antibodies_column are adding up to total_population_column.
    check the design weights antibodies are all are positive.
    check there are no missing design weights antibodies.
    check if they are the same across cis_area_code_20 by sample groups (by sample_source).

    Parameters
    ----------
    df
    """
    # part 1: check the dweights_antibodies_column are adding up to total_population_column
    assert sum(list(df.select(dweights_antibodies_column).toPandas()[dweights_antibodies_column])) == sum(
        list(df.select(total_population_column).toPandas()[total_population_column])
    )

    df = df.withColumn(
        "check_2-dweights_positive_notnull",
        F.when(F.col(total_population_column) > 1, 1).when(F.col(total_population_column).isNotNull(), 1),
    )

    # TODO part 4: check if they are the same across cis_area_code_20 by sample groups (by sample_source)

    return df


# 1178
def function_name_1178(
    hh_samples_df: DataFrame,
    df_extract_by_country: DataFrame,
    table_name: str,
    required_extracts_column_list: List[str],
    mandatory_extracts_column_list: List[str],
    population_join_column: str,
) -> DataFrame:
    """
    Parameters
    ----------
    hh_samples_df
    df_extract_by_country
    table_name
    required_extracts_column_list
    mandatory_extracts_column_list
    population_join_column: Only swab/antibody
    """
    # STEP 1 - create: extract_dataset as per requirements
    # (i.e extract dataset for calibration from survey_data dataset (individual level))
    df = extract_from_table(table_name).select(*required_extracts_column_list)

    # STEP 2 -  check there are no missing values
    for var in mandatory_extracts_column_list:
        df = df.withColumn(
            "check_if_missing", F.when(F.col(var).isNull(), 1)
        )  # TODO: check if multiple columns are needed

    # STEP 3 -  create household level of the extract to be used in calculation of non response factor
    df = (
        df.withColumn("response_indicator", F.when(F.col("participant_id").isNotNull(), 1).otherwise(None))
        .drop("participant_id")
        .dropDuplicates("ons_household_id")
    )

    # STEP 4 - merge hh_samples_df (36 processing step) and household level extract (69 processing step)
    # TODO: check if population_by_country comes by country separated or together
    hh_samples_df = hh_samples_df.join(
        df=df.select(*required_extracts_column_list, "check_if_missing"), on="ons_household_id", how="left"
    )

    hh_samples_df = hh_samples_df.join(
        df=df_extract_by_country, on="population_country_{population_join_column}", how="left"
    )

    return df


# 1178 needed filtering function
def dataset_generation(df, logic):
    return df.where(logic)


# 1179
def function1179_1(df):
    """
    Parameters
    ----------
    df
    """
    # A.1 group households  considering  values to index of multiple deprivation
    map_index_multiple_deprivation_group_country = {
        "england": {0: 1, 6569: 2, 13138: 3, 19707: 4, 26276: 5},
        "wales": {0: 1, 764: 2, 1146: 3, 1528: 4, 1529: 5},
        "scotland": {0: 1, 2790: 2, 4185: 3, 5580: 4, 5581: 5},
        "northen_ireland": {0: 1, 356: 2, 534: 3, 712: 4, 713: 5},
    }

    for country in map_index_multiple_deprivation_group_country.keys():
        df = assign_named_buckets(
            df=df,
            reference_column="index_multiple_deprivation",
            column_name_to_assign="index_multiple_deprivation_group",
            map=map_index_multiple_deprivation_group_country[country],
        )

    return df


# 1179
def assign_total_sampled_households_cis_imd_addressbase(df: DataFrame, country_column: str) -> DataFrame:
    """
    Parameters
    ----------
    df
    country_column: For northen_ireland, use country, otherwise use cis_area_code_20
    """
    window_list = ["sample_addressbase_indicator", country_column, "index_multiple_deprivation_group"]

    w = Window.partitionBy(*window_list)

    df = df.withColumn("total_sampled_households_cis_imd_addressbase", F.count(F.col("ons_household_id")).over(w))

    df = df.withColumn(
        "total_responded_households_cis_imd_addressbase",
        F.when(F.col("interim_participant_id") == 1, F.count(F.col("ons_household_id"))).over(w),
    )

    # B.1 calculate raw non_response_factor by dividing total respondent household
    # to total sampled households (calculated above A.2, A.3)
    df = df.withColumn(
        "raw_non_response_factor",
        (
            F.col("total_sampled_households_cis_imd_addressbase")
            / F.col("total_responded_households_cis_imd_addressbase")
        ),
    )
    return df


def function1179_3(df: DataFrame, country_list: List[str], test_type: str) -> DataFrame:
    """
    Parameters
    ----------
    """
    # B.2 calculate scaled non_response_factor by dividing raw_non_response_factor
    # to the mean of raw_non_response_factor
    w_country = Window.partitionBy(country_list)

    raw_non_response_factor_mean = df.select(F.mean(F.col("raw_non_response_factor"))).collect()[0][0]

    df = df.withColumn(
        "mean_raw_non_response_factor",
        (F.col("raw_non_response_factor") / raw_non_response_factor_mean).over(w_country),
    )

    df = df.withColumn("scaled_non_response_factor", F.col("raw_non_response_factor") / raw_non_response_factor_mean)

    # B.3 calculate bounded non_response_factor by adjusting the values of the scaled
    # non_response factor to be contained in a certain, pre-defined range
    df = df.withColumn(
        "bounded_non_response_factor",
        F.when(F.col("scaled_non_response_factor") < 0.5, 0.6).when(F.col("scaled_non_response_factor") > 2.0, 1.8),
    )  # TODO: is this in a country window ?

    # C1. adjust design weights by the non_response_factor by multiplying the desing
    # weights with the non_response factor
    df = df.withColumn(
        "household_level_designweight_adjusted_swab",
        F.when(
            # TODO: when  data extracted is a swab or longcovid type of dataset
            True & F.col("response_indicator") == 1,
            F.col("household_level_designweight_swab") * F.col("bounded_non_response_factor"),
        ).over(w_country),
    )

    df = df.withColumn(
        "household_level_designweight_adjusted_antibodies",
        F.when(
            # TODO: when  data extracted is a antibodies type of dataset
            True & F.col("response_indicator") == 1,
            F.col("household_level_designweight_antibodies") * F.col("bounded_non_response_factor"),
        ).over(w_country),
    )

    # D.1 calculate the scaling factor and then apply the scaling factor the
    # design weights adjusted by the non response factor.
    # If there is a swab or long covid type dataset, then use the population
    # totlas for swab and the naming will have "swab" incorporated.
    # Same for antibodies type dataset

    # sum_adjusted_design_weight_swab or antibodies
    df = df.withColumn(
        "sum_adjusted_design_weight_" + test_type,
        F.when(F.col("response_indicator") == 1, F.col("household_level_designweight_adjusted_" + test_type)).over(
            w_country
        ),
    )

    # scaling_factor_adjusted_design_weight_swab or antibodies
    sum_adjusted_design_weight = df.select(F.sum(F.col("adjusted_design_weight_" + test_type))).collect()[0][0]

    df = df.withColumn(
        "scaling_factor_adjusted_design_weight_" + test_type,
        F.when(
            F.col("response_indicator") == 1, (F.col("population_country_" + test_type) / sum_adjusted_design_weight)
        ).over(w_country),
    )

    # scaled_design_weight_adjusted_swab or antibodies
    df = df.withColumn(
        "scaled_design_weight_adjusted_" + test_type,
        F.when(
            F.col("response_indicator") == 1,
            (
                F.col("scaling_factor_adjusted_design_weight_" + test_type)
                * F.col("household_level_designweight_adjusted_" + test_type)
            ),
        ).over(w_country),
    )
    return df


def precalibration_checkpoints(df, test_type, population_totals, dweight_list):
    """
    Parameters
    ----------
    df
    test_type
    population_totals
    dweight_list
    """
    # TODO: create a column
    # check_1: The design weights are adding up to total population
    check_1 = df.select(F.sum(F.col("scaled_design_weight_adjusted_") + test_type)).collect()[0][0] == population_totals

    # check_2 and check_3: The  design weights are all are positive AND check there are no missing design weights
    for dweight in dweight_list:
        df = df.withColumn("not_positive_or_null", F.when((F.col(dweight) < 0) | (F.col(dweight).isNull()), 1))
    check_2_3 = [1] in df.select("not_positive_or_null").distinct().collect()[0]

    # check_4: if they are the same across cis_area_code_20 by sample groups (by sample_source)
    check_4 = True

    return check_1, check_2_3, check_4


# 1180
def function_1180(df):
    """
    Parameters
    ----------
    df
    """
    # A.1 re-code the region_code values, by replacing the alphanumeric code with numbers from 1 to 12
    region_code_lookup_df = spark_session.createDataFrame(
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
        schema="reference_1 string, outcome integer",
    )
    df = assign_from_lookup(
        df=df,
        column_name_to_assign="interim_region_code",
        reference_columns=["reference_1"],
        lookup_df=region_code_lookup_df,
    )

    # A.2 re-code sex variable replacing string with integers
    sex_code_lookup_df = spark_session.createDataFrame(
        data=[
            ("male", 1),
            ("female", 2),
        ],
        schema="reference_1 string, outcome integer",
    )
    df = assign_from_lookup(
        df=df,
        column_name_to_assign="interim_sex",
        reference_columns=["reference_1"],
        lookup_df=sex_code_lookup_df,
    )

    # A.3 create age groups considering certain age boundaries,
    # as needed for calibration weighting of swab data
    map_age_at_visit = {2: 1, 12: 2, 17: 3, 25: 4, 35: 5, 50: 6, 70: 7}

    df = assign_named_buckets(
        df=df,
        reference_column="age_at_visit",
        column_name_to_assign="age_group_swab",
        map=map_age_at_visit,
    )

    # A.4 create age groups considering certain age boundaries,
    # needed for calibration weighting of antibodies data
    df = assign_named_buckets(
        df=df,
        reference_column="age_at_visit",
        column_name_to_assign="age_group_antibodies",
        map=map_age_at_visit,
    )

    # A.5 Creating first partition(p1)/calibration variable
    return df


# 1180
def create_calibration_var(df, dataset, dataset_type):
    """
    Parameters
    ----------
    df
    """
    df = df.withColumn(
        "p1_swab_longcovid_england",
        (F.when(F.col("country_name") == "england")) & (dataset_type),
        ((F.col("interim_region_code") - 1) * 14 + (F.col("interim_sex") - 1) * 7 + F.col("age_group_swab")),
    )

    df = df.withColumn(
        "p1_swab_longcovid_wales_scot_ni",
        (
            (F.when(F.col("country_name") == "wales"))
            | (F.when(F.col("country_name") == "scotland"))
            | (F.when(F.col("country_name") == "northern_ireland"))  # TODO: double-check name
        )
        & (dataset_type),
        ((F.col("interim_sex") - 1) * 7 + F.col("age_group_swab")),
    )

    df = df.withColumn(
        "p1_for_antibodies_evernever_engl",
        (F.when(F.col("country_name") == "england")) & (dataset_type),
        ((F.col("interim_region_code") - 1) * 10 + (F.col("interim_sex") - 1) * 5 + F.col("age_group_antibodies")),
    )

    df = df.withColumn(
        "p1_for_antibodies_28daysto_engl",
        (F.when(F.col("country_name") == "england")) & (dataset_type),
        ((F.col("interim_sex") - 1) * 5 + F.col("age_group_antibodies")),
    )

    df = df.withColumn(
        "p1_for_antibodies_wales_scot_ni",
        (
            (F.when(F.col("country_name") == "wales"))
            | (F.when(F.col("country_name") == "scotland"))
            | (F.when(F.col("country_name") == "northern_ireland"))  # TODO: double-check name
        )
        & (dataset_type),
        ((F.col("interim_sex") - 1) * 5 + F.col("age_group_antibodies")),
    )

    # A.6 Create second partition (p2)/calibration variable
    df = df.withColumn(
        "p2_for_antibodies",
        ((F.when(F.col("country_name") == "wales")) | (F.when(F.col("country_name") == "england"))) & (dataset_type),
        (F.col("ethnicity_white") + 1),
    )

    df = df.withColumn(
        "p3_for_antibodies_28daysto_engl",
        (F.when(F.col("country_name") == "england")) & (dataset_type) & (F.col("age_at_visit") >= 16),
        F.col("interim_region_code"),
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
            england_swab_evernever, england_swab_14days, england_long_covid_28days, england_long_covid_42days
        2 for
            wales_swab_evernever, wales_swab_14days, wales_long_covid_28days, wales_long_covid_42days,
            scotland_swab_evernever, scotland_swab_14days, scotland_long_covid_28days, scotland_long_covid_42days,
            northen_ireland_swab_evernever, northen_ireland_swab_14days, northen_ireland_long_covid_28days,
            northen_ireland_long_covid_42days
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
                "country_name",
                "participant_id",
                "scaled_design_weight_adjusted_swab",
                "p1_swab_longcovid_england",
            ],
            "create_dataset": [
                "england_swab_evernever",
                "england_swab_14days",
                "england_long_covid_24days",
                "england_long_covid_42days",
            ],
        },
        2: {
            "variable": ["wales", "scotland", "northen_ireland"],
            "keep_var": [
                "country_name",
                "participant_id",
                "scaled_design_weight_adjusted_swab",
                "p1_swab_longcovid_wales_scot_ni",
            ],
            "create_dataset": [
                "wales_swab_evernever",
                "wales_swab_14days",
                "wales_long_covid_24days",
                "wales_long_covid_42days",
                "scotland_swab_evernever",
                "scotland_swab_14days",
                "scotland_long_covid__24days",
                "scotland_long_covid_42days",
                "northen_ireland_swab_evernever",
                "northen_ireland_swab_14days",
                "northen_ireland_long_covid_24days",
                "northen_ireland_long_covid_42days",
            ],
        },
        3: {
            "variable": ["england"],
            "keep_var": [
                "country_name",
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
                "country_name",
                "participant_id",
                "scaled_design_weight_adjusted_antibodies",
                "p1_for_antibodies_28daysto_engl",
                "p2_for_antibodies",
                "p3_for_antibodies",
            ],
            "create_dataset": ["england_antibodies_28daysto"],
        },
        5: {
            "variable": ["wales"],
            "keep_var": [
                "country_name",
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
                "country_name",
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

    df = df.where(F.col("country_name").isin(dataset_dict[processing_step]["variable"])).select(
        *dataset_dict[processing_step]["keep_var"]
    )

    # df.where(F.col('country_name').isin(dataset_dict[processing_step]['variable']))
    # TODO: create datasets dataset_dict[processing_step]['create_dataset']
    return df
