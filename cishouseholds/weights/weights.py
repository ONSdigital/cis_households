import re
from typing import List
from typing import Union

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from cishouseholds.derive import assign_count_by_group
from cishouseholds.derive import assign_distinct_count_in_group
from cishouseholds.derive import assign_filename_column
from cishouseholds.derive import count_value_occurrences_in_column_subset_row_wise
from cishouseholds.edit import clean_postcode
from cishouseholds.merge import union_multiple_tables
from cishouseholds.weights.derive import assign_sample_new_previous
from cishouseholds.weights.derive import assign_tranche_factor
from cishouseholds.weights.edit import join_on_existing
from cishouseholds.weights.edit import null_to_value

# from cishouseholds.weights.edit import clean_df

# from cishouseholds.weights.derive import get_matches

# notes:
# validation checks relate to final dweights for entire column for both datasets (each hh must have a dweight)


def generate_weights(
    household_level_populations_df: DataFrame,
    master_sample_df: DataFrame,
    old_sample_df: DataFrame,
    new_sample_df: DataFrame,
    tranche_df: DataFrame,
    postcode_lookup_df: DataFrame,
    country_lookup_df: DataFrame,
    lsoa_cis_lookup_df: DataFrame,
    first_run: bool,
):
    # initialise lookup dataframes
    df = join_and_process_lookups(
        household_level_populations_df,
        master_sample_df,
        old_sample_df,
        new_sample_df,
        postcode_lookup_df,
        country_lookup_df,
        lsoa_cis_lookup_df,
        first_run,
    )

    # transform sample files
    df = assign_sample_new_previous(df, "sample_new_previous", "date_sample_created", "batch_number")

    if tranche_df is not None:
        tranche_df = assign_filename_column(tranche_df, "tranche_source_file")
        tranche_df = tranche_df.withColumn("TRANCHE_BARCODE_REF", F.col("ons_household_id"))

        # df = df.join(tranche_df, on="ons_household_id", how="leftouter").drop("UAC")
        df = join_on_existing(df=df, df_to_join=tranche_df, on=["ons_household_id"]).drop("UAC")
        df = assign_tranche_factor(
            df=df,
            column_name_to_assign="tranche_factor",
            barcode_column="ons_household_id",
            barcode_ref_column="TRANCHE_BARCODE_REF",
            tranche_column="tranche",
            group_by_columns=["cis_area_code_20", "enrolment_date"],
        )
    else:
        df = df.withColumn("tranche_eligible_households", F.lit("No"))
        df = df.withColumn("number_eligible_households_tranche_bystrata_enrolment", F.lit(None).cast("integer"))
        df = df.withColumn("number_sampled_households_tranche_bystrata_enrolment", F.lit(None).cast("integer"))

    cis_window = Window.partitionBy("cis_area_code_20")
    df = swab_weight_wrapper(
        df=df, household_level_populations_df=household_level_populations_df, cis_window=cis_window
    )
    scenario_string = chose_scenario_of_dweight_for_antibody_different_household(
        df=df,
        tranche_eligible_indicator="tranche_eligible_households",
        eligibility_pct_column="eligibility_pct",
        n_eligible_hh_tranche_bystrata_column="number_eligible_households_tranche_bystrata_enrolment",
        n_sampled_hh_tranche_bystrata_column="number_sampled_households_tranche_bystrata_enrolment",
    )
    df = antibody_weight_wrapper(df=df, cis_window=cis_window, scenario=scenario_string)  # type: ignore
    df = carry_forward_design_weights(
        df=df,
        scenario=scenario_string,  # type: ignore
        groupby_column="cis_area_code_20",
        household_population_column="number_of_households_by_cis_area",
    )
    # validate_design_weights_or_precal(
    #     df=df,
    #     num_households_by_cis_column="number_of_households_by_cis_area",
    #     num_households_by_country_column="number_of_households_by_country",
    #     swab_weight_column="scaled_design_weight_swab_nonadjusted",
    #     antibody_weight_column="scaled_design_weight_antibodies_nonadjusted",
    #     group_by_columns=["cis_area_code_20"],
    # )
    return df


def join_and_process_lookups(
    household_level_populations_df: DataFrame,
    master_sample_df: DataFrame,
    old_sample_df: DataFrame,
    new_sample_df: DataFrame,
    postcode_lookup_df: DataFrame,
    country_lookup_df: DataFrame,
    lsoa_cis_lookup_df: DataFrame,
    first_run: bool,
) -> DataFrame:
    """
    Add data from the additional lookup files to main dataset
    """
    master_sample_df = clean_postcode(master_sample_df, "postcode").withColumn(
        "ons_household_id", F.regexp_replace(F.col("ons_household_id"), r"[^\d]{1,}", "")
    )

    postcode_lookup_df = clean_postcode(postcode_lookup_df, "postcode")

    old_sample_df = recode_columns(old_sample_df, new_sample_df, household_level_populations_df)
    old_sample_df = old_sample_df.withColumn("date_sample_created", F.lit("2021/11/30"))
    new_sample_df = assign_filename_column(new_sample_df, "sample_source_file")

    new_sample_df = new_sample_df.join(
        master_sample_df.select("ons_household_id", "sample_type").withColumn(
            "ons_household_id", F.regexp_replace(F.col("ons_household_id"), r"^.{4}", "")
        ),
        on="ons_household_id",
        how="left",
    ).withColumn("date_sample_created", F.lit("2021/12/06"))

    new_sample_df = new_sample_df.withColumn(
        "sample_addressbase_indicator", F.when(F.col("sample_type").isin(["AB", "AB-Trial"]), 1).otherwise(0)
    )

    if first_run:
        old_sample_df = assign_filename_column(old_sample_df, "sample_source_file")
        df = union_multiple_tables([old_sample_df, new_sample_df])
    else:
        df = new_sample_df

    df = df.join(
        master_sample_df.select("postcode", "ons_household_id"),
        on="ons_household_id",
        how="left",
    )

    df = df.join(
        lsoa_cis_lookup_df.select("lower_super_output_area_code_11", "cis_area_code_20"),
        on="lower_super_output_area_code_11",
        how="left",
    )
    df = df.drop("rural_urban_classification_11", "country_code_12").join(
        postcode_lookup_df.select(
            "postcode", "rural_urban_classification_11", "output_area_code_11", "country_code_12"
        ).distinct(),
        on="postcode",
        how="left",
    )
    df = df.join(
        country_lookup_df.select("country_code_12", "country_name_12").distinct(), on="country_code_12", how="left"
    )
    df = df.withColumn("batch_number", F.lit(1))
    if first_run:
        return df

    return union_multiple_tables([old_sample_df, df])


def recode_columns(old_df: DataFrame, new_df: DataFrame, hh_info_df: DataFrame) -> DataFrame:
    """
    Recode and rename old column values in sample data if change detected in new sample data
    Paramaters
    """
    old_lsoa = list(filter(re.compile(r"^lower_super_output_area_code_\d{1,}$").match, old_df.columns))[0]
    new_lsoa = list(filter(re.compile(r"^lower_super_output_area_code_\d{1,}$").match, new_df.columns))[0]
    info_lsoa = list(filter(re.compile(r"^lower_super_output_area_code_\d{1,}$").match, hh_info_df.columns))[0]
    info_cis = list(filter(re.compile(r"^cis_area_code_\d{1,}$").match, hh_info_df.columns))[0]
    if old_lsoa != new_lsoa:
        old_df = clean_postcode(old_df, "postcode")
        hh_info_df = clean_postcode(hh_info_df, "postcode")
        old_df = old_df.join(
            hh_info_df.select(info_lsoa, info_cis, "postcode")
            .withColumnRenamed(info_lsoa, "lsoa_from_lookup")
            .withColumnRenamed(info_cis, "cis_from_lookup"),
            on="postcode",
            how="left",
        )
        old_df = old_df.withColumn(old_lsoa, "lsoa_from_lookup").withColumnRenamed(old_lsoa, new_lsoa).drop(old_lsoa)
    return old_df


def household_level_populations(
    address_lookup: DataFrame, postcode_lookup: DataFrame, lsoa_cis_lookup: DataFrame, country_lookup: DataFrame
) -> DataFrame:
    """
    1. join address base extract with NSPL by postcode to get LSOA 11 and country 12
    2. join LSOA to CIS lookup, by LSOA 11 to get CIS area 20
    3. join country lookup by country_code to get country names
    4. calculate household counts by CIS area and country

    N.B. Expects join keys to be deduplicated.
    """
    address_lookup = clean_postcode(address_lookup, "postcode")
    postcode_lookup = clean_postcode(postcode_lookup, "postcode")
    df = address_lookup.join(F.broadcast(postcode_lookup), on="postcode", how="left")
    df = df.join(F.broadcast(lsoa_cis_lookup), on="lower_super_output_area_code_11", how="left")
    df = df.join(F.broadcast(country_lookup), on="country_code_12", how="left")
    df = assign_count_by_group(df, "number_of_households_by_cis_area", ["cis_area_code_20"])
    df = assign_count_by_group(df, "number_of_households_by_country", ["country_code_12"])

    return df


# Wrapper
def swab_weight_wrapper(df: DataFrame, household_level_populations_df: DataFrame, cis_window: Window):
    """
    Wrapper function to calculate swab design weights
    """
    # Swab weights
    df = calculate_dweight_swabs(
        df=df,
        household_level_populations_df=household_level_populations_df,
        column_name_to_assign="raw_design_weights_swab",
        sample_type_column="sample_new_previous",
        group_by_columns=["cis_area_code_20", "sample_new_previous"],
        barcode_column="ons_household_id",
        previous_dweight_column="household_level_designweight_swab",
    )
    df = calculate_generic_dweight_variables(
        df=df,
        design_weight_column="raw_design_weights_swab",
        num_eligible_hosusehold_column="number_eligible_household_sample",
        groupby_columns=["cis_area_code_20", "sample_new_previous"],
        test_type="swab",
        cis_window=cis_window,
    )
    df = calculate_combined_dweight_swabs(
        df=df,
        design_weight_column="combined_design_weight_swab",
        num_households_column="number_of_households_by_cis_area",
        cis_window=cis_window,
    )
    return df


# Wrapper
def antibody_weight_wrapper(df: DataFrame, cis_window: Window, scenario: str = "A"):
    # Antibody weights
    if scenario in "AB":
        design_weight_column = "raw_design_weight_antibodies_ab"
        df = calculate_scenario_ab_antibody_dweights(
            df=df,
            column_name_to_assign=design_weight_column,
            hh_dweight_antibodies_column="household_level_designweight_antibodies",
            sample_new_previous_column="sample_new_previous",
            scaled_dweight_swab_nonadjusted_column="scaled_design_weight_swab_nonadjusted",
        )
    elif scenario == "C":
        design_weight_column = "raw_design_weight_antibodies_c"
        df = calculate_scenario_c_antibody_dweights(
            df=df,
            column_name_to_assign=design_weight_column,
            sample_new_previous_column="sample_new_previous",
            tranche_eligible_column="tranche_eligible_households",
            tranche_num_column="tranche",
            design_weight_column="scaled_design_weight_swab_nonadjusted",
            tranche_factor_column="tranche_factor",
            household_dweight_column="household_level_designweight_antibodies",
        )
    df = calculate_generic_dweight_variables(
        df=df,
        design_weight_column=design_weight_column,
        num_eligible_hosusehold_column="number_eligible_household_sample",
        groupby_columns=["cis_area_code_20", "sample_new_previous"],
        test_type="antibody",
        cis_window=cis_window,
    )
    return df


def calculate_dweight_swabs(
    df: DataFrame,
    household_level_populations_df: DataFrame,
    column_name_to_assign: str,
    sample_type_column: str,
    group_by_columns: List[str],
    barcode_column: str,
    previous_dweight_column: str,
):
    """
    Assign design weight for swabs sample by applying the ratio between number_of_households_population_by_cis and
    number_eligible_household_sample when the sample type is "new"
    """
    window = Window.partitionBy(*group_by_columns)
    df = df.drop("number_of_households_by_cis_area").join(
        household_level_populations_df.select(
            "number_of_households_by_cis_area", "number_of_households_by_country", "cis_area_code_20"
        ).distinct(),
        on="cis_area_code_20",
        how="left",
    )
    df = df.withColumn("number_eligible_household_sample", F.approx_count_distinct(barcode_column).over(window))
    df = df.withColumn(
        column_name_to_assign,
        F.when(
            F.col(sample_type_column) == "new",
            F.col("number_of_households_by_cis_area") / F.col("number_eligible_household_sample"),
        ).otherwise(F.col(previous_dweight_column)),
    )
    return df


# 1166
def calculate_generic_dweight_variables(
    df: DataFrame,
    design_weight_column: str,
    num_eligible_hosusehold_column: str,
    groupby_columns: List[str],
    test_type: str,
    cis_window: Window,
) -> DataFrame:
    """
    calculate variables common to design weights
    """
    window = Window.partitionBy(*groupby_columns)
    df = df.withColumn(f"sum_raw_design_weight_{test_type}_cis", F.sum(design_weight_column).over(window))
    df = df.withColumn(f"standard_deviation_raw_design_weight_{test_type}", F.stddev(design_weight_column).over(window))
    df = df.withColumn(f"mean_raw_design_weight_{test_type}", F.mean(design_weight_column).over(window))

    df = df.withColumn(
        f"coefficient_variation_design_weight_{test_type}",
        (F.col(f"standard_deviation_raw_design_weight_{test_type}") / F.col(f"mean_raw_design_weight_{test_type}")),
    )
    df = df.withColumn(
        f"design_effect_weight_{test_type}", 1 + F.pow(F.col(f"coefficient_variation_design_weight_{test_type}"), 2)
    )
    df = df.withColumn(
        f"effective_sample_size_design_weight_{test_type}",
        F.col(num_eligible_hosusehold_column) / F.col(f"design_effect_weight_{test_type}"),
    )
    df = null_to_value(df, f"effective_sample_size_design_weight_{test_type}")

    df = df.withColumn(
        f"sum_effective_sample_size_design_weight_{test_type}",
        F.sum(f"effective_sample_size_design_weight_{test_type}").over(cis_window),
    )
    df = df.withColumn(
        f"combining_factor_design_weight_{test_type}",
        F.col(f"effective_sample_size_design_weight_{test_type}")
        / F.col(f"sum_effective_sample_size_design_weight_{test_type}"),
    )
    df = df.withColumn(
        f"combined_design_weight_{test_type}",
        F.col(f"combining_factor_design_weight_{test_type}") * F.col(design_weight_column),
    )
    return df


# 1166
def calculate_combined_dweight_swabs(
    df: DataFrame, design_weight_column: str, num_households_column: str, cis_window: Window
) -> DataFrame:
    """
    Apply logic to derive overall design weights
    """
    df = df.withColumn("sum_combined_design_weight_swab", F.sum(design_weight_column).over(cis_window))
    df = df.withColumn(
        "scaling_factor_combined_design_weight_swab",
        F.col(num_households_column) / F.col("sum_combined_design_weight_swab"),
    )
    df = df.withColumn(
        "scaled_design_weight_swab_nonadjusted",
        F.col("scaling_factor_combined_design_weight_swab") * F.col(design_weight_column),
    )
    return df


class DesignWeightError(Exception):
    pass


# 1166
def validate_design_weights_or_precal(
    df: DataFrame,
    num_households_by_cis_column: str,
    num_households_by_country_column: str,
    swab_weight_column: str,
    antibody_weight_column: str,
    group_by_columns: List[str],
):
    """
    Validate the derived design weights by checking 3 conditions are true:
    - design weights add to household population total
    - no design weights are negative
    - no design weights are missing
    - dweights consistent by cis area
    """
    # import pdb; pdb.set_trace()
    window = Window.partitionBy(F.lit(1))
    columns = [col for col in df.columns if "weight" in col and list(df.select(col).dtypes[0])[1] == "double"]
    # check 1.1
    df = df.withColumn(
        "CHECK1s",
        F.when(F.sum(swab_weight_column).over(window) == F.col(num_households_by_cis_column), 0).otherwise(1),
    )
    # check 1.2
    df = df.withColumn(
        "CHECK1a",
        F.when(F.sum(antibody_weight_column).over(window) == F.col(num_households_by_country_column), 0).otherwise(1),
    )
    # check 2
    df = df.withColumn("CHECK2", F.when(F.least(*columns) < 0, 1).otherwise(0))
    # check 3
    df = count_value_occurrences_in_column_subset_row_wise(df, "NUM_NULLS", columns, None)
    df = df.withColumn("CHECK3", F.when(F.col("NUM_NULLS") != 0, 1).otherwise(0)).drop("NUM_NULLS")
    # check 4
    df = assign_distinct_count_in_group(df, "TEMP_DISTINCT_COUNT", columns, group_by_columns)
    df = df.withColumn("CHECK4", F.when(F.col("TEMP_DISTINCT_COUNT") != 1, 1).otherwise(0)).drop("TEMP_DISTINCT_COUNT")
    check_1_s = True if df.filter(F.col("CHECK1s") == 1).count() == 0 else False
    check_1_a = True if df.filter(F.col("CHECK1a") == 1).count() == 0 else False
    check_2 = True if df.filter(F.col("CHECK2") == 1).count() == 0 else False
    check_3 = True if df.filter(F.col("CHECK3") == 1).count() == 0 else False
    check_4 = True if df.filter(F.col("CHECK4") == 1).count() == 0 else False
    print("CHECKS: ", check_1_a, check_1_s, check_2, check_3, check_4)  # functional
    # if not (check_1_a or check_1_s):
    #     raise DesignWeightError("check_1: The design weights are NOT adding up to total population.")
    # if not check_2:
    #     raise DesignWeightError("check_2: The design weights are NOT all are positive.")
    # if not check_3:
    #     raise DesignWeightError("check_3 There are no missing design weights.")
    # if not check_4:
    #     raise DesignWeightError("check_4: There are weights that are NOT the same across sample groups.")


# 1167
def chose_scenario_of_dweight_for_antibody_different_household(
    df: DataFrame,
    eligibility_pct_column: str,
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
    tranche_eligible_indicator
    household_samples_dataframe
    n_eligible_hh_tranche_bystrata_column
    n_sampled_hh_tranche_bystrata_column
    """
    if tranche_eligible_indicator not in df.columns:  # TODO: not in household_samples_dataframe?
        return "A"
    df = df.withColumn(
        eligibility_pct_column,
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
                * (F.col(n_eligible_hh_tranche_bystrata_column) - F.col(n_sampled_hh_tranche_bystrata_column))
                / F.col(n_sampled_hh_tranche_bystrata_column)
            ),
        )
        .otherwise(None),  # TODO: check this
    )
    eligibility_pct_val = df.select(eligibility_pct_column).collect()[0][0]
    df = df.drop(eligibility_pct_column)
    if eligibility_pct_val == 0:
        return "B"
    else:
        return "C"


# 1168
def calculate_scenario_ab_antibody_dweights(
    df: DataFrame,
    column_name_to_assign: str,
    hh_dweight_antibodies_column: str,
    sample_new_previous_column: str,
    scaled_dweight_swab_nonadjusted_column: str,
) -> DataFrame:
    """
    Parameters
    ----------
    df
    column_name_to_assign
    hh_dweight_antibodies_column
    sample_new_previous_column
    scaled_dweight_swab_nonadjusted_column
    """

    df = df.withColumn(
        column_name_to_assign,
        F.when(
            (F.col(hh_dweight_antibodies_column).isNotNull()) & (F.col(sample_new_previous_column) == "previous"),
            F.col(hh_dweight_antibodies_column),
        ).otherwise(F.col(scaled_dweight_swab_nonadjusted_column)),
    )
    return df


# 1169
def calculate_scenario_c_antibody_dweights(
    df: DataFrame,
    column_name_to_assign: str,
    sample_new_previous_column: str,
    tranche_eligible_column: str,
    tranche_num_column: str,
    design_weight_column: str,
    tranche_factor_column: str,
    household_dweight_column: str,
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
    df,
    column_name_to_assign,
    sample_new_previous_column,
    tranche_eligible_column,
    tranche_num_column,
    design_weight_column,
    tranche_factor_column,
    household_dweight_column,
    """
    max_value = df.agg({tranche_num_column: "max"}).first()[0]

    df = df.withColumn(
        column_name_to_assign,
        F.when(
            (F.col(sample_new_previous_column) == "previous")
            & (F.col(tranche_eligible_column) == "Yes")
            & (F.col(tranche_num_column) == max_value),
            F.col(design_weight_column) * F.col(tranche_factor_column),
        )
        .when(
            (F.col(sample_new_previous_column) == "previous")
            & (F.col(tranche_num_column) != max_value)
            & (F.col(tranche_eligible_column) != "Yes"),
            F.col(household_dweight_column),
        )
        .when((F.col(sample_new_previous_column) == "new"), F.col(design_weight_column)),
    )
    return df


def carry_forward_design_weights(df: DataFrame, scenario: str, groupby_column: str, household_population_column: str):
    """
    Use scenario lookup to apply dependent function to carry forward design weights variable
    to current dataframe
    """
    window = Window.partitionBy(groupby_column)
    scenario_carry_forward_lookup = {
        "A": "raw_design_weight_antibodies_ab",
        "B": "raw_design_weight_antibodies_ab",
        "C": "combined_design_weight_antibody",
    }
    df = df.withColumn("carryforward_design_weight_antibodies", F.col(scenario_carry_forward_lookup[scenario]))
    df = df.withColumn(
        "sum_carryforward_design_weight_antibodies", F.sum("carryforward_design_weight_antibodies").over(window)
    )
    df = df.withColumn(
        "scaling_factor_carryforward_design_weight_antibodies",
        F.col(household_population_column) / F.col("sum_carryforward_design_weight_antibodies"),
    )
    df = df.withColumn(
        "scaled_design_weight_antibodies_nonadjusted",
        F.col("scaling_factor_carryforward_design_weight_antibodies") * F.col("carryforward_design_weight_antibodies"),
    )
    return df
