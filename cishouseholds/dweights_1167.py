from typing import List
from typing import Union

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

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
def function(
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


def function1179_2(df: DataFrame, country_column: str) -> DataFrame:
    """
    Parameters
    ----------
    df
    country_column: For northen_ireland, use country, otherwise use cis_area_code_20
    """
    window_list = "sample_addressbase_indicator", country_column, "index_multiple_deprivation_group"

    w = Window.partitionBy(window_list)

    df = df.withColumn("total_sampled_households_cis_imd_addressbase", F.count(F.col("ons_household_id")).over(w))

    df = df.withColumn(
        "total_responded_households_cis_imd_addressbase",
        F.when(F.col("interim_participant_id") == 1, F.count(F.col("ons_household_id"))).over(w),
    )
    return df
