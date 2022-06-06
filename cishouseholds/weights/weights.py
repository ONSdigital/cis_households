import re
from typing import List
from typing import Union

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from cishouseholds.derive import assign_count_by_group
from cishouseholds.derive import assign_distinct_count_in_group
from cishouseholds.derive import assign_filename_column
from cishouseholds.edit import clean_postcode
from cishouseholds.merge import union_multiple_tables
from cishouseholds.weights.derive import assign_sample_new_previous
from cishouseholds.weights.derive import assign_tranche_factor
from cishouseholds.weights.edit import join_on_existing
from cishouseholds.weights.edit import null_to_value


def generate_weights(
    household_level_populations_df: DataFrame,
    master_sample_df: DataFrame,
    old_sample_df: DataFrame,
    new_sample_df: DataFrame,
    new_sample_source_name: str,
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
        new_sample_source_name,
        postcode_lookup_df,
        country_lookup_df,
        lsoa_cis_lookup_df,
        first_run,
    )

    df = assign_sample_new_previous(df, "sample_new_previous", "date_sample_created", "batch_number")

    if tranche_df is not None:
        tranche_df = assign_filename_column(tranche_df, "tranche_source_file")
        tranche_df = tranche_df.withColumn("TRANCHE_BARCODE_REF", F.col("ons_household_id"))

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
        df = df.withColumn("number_eligible_households_tranche_by_strata_enrolment", F.lit(None).cast("integer"))
        df = df.withColumn("number_sampled_households_tranche_by_strata_enrolment", F.lit(None).cast("integer"))

    cis_window = Window.partitionBy("cis_area_code_20")
    df = swab_weight_wrapper(
        df=df, household_level_populations_df=household_level_populations_df, cis_window=cis_window
    )
    scenario_string = chose_scenario_of_design_weight_for_antibody_different_household(
        df=df,
        tranche_eligible_indicator="tranche_eligible_households",
        eligibility_pct_column="eligibility_pct",
        n_eligible_hh_tranche_by_strata_column="number_eligible_households_tranche_by_strata_enrolment",
        n_sampled_hh_tranche_by_strata_column="number_sampled_households_tranche_by_strata_enrolment",
    )
    df = antibody_weight_wrapper(df=df, cis_window=cis_window, scenario=scenario_string)  # type: ignore
    df = carry_forward_design_weights(
        df=df,
        scenario=scenario_string,  # type: ignore
        groupby_column="cis_area_code_20",
        household_population_column="number_of_households_by_cis_area",
    )
    try:
        validate_design_weights(
            df=df,
            num_households_by_cis_column="number_of_households_by_cis_area",
            num_households_by_country_column="number_of_households_by_country",
            swab_weight_column="scaled_design_weight_swab_non_adjusted",
            antibody_weight_column="scaled_design_weight_antibodies_non_adjusted",
            cis_area_column="cis_area_code_20",
            country_column="country_code_12",
            swab_group_by_columns=["cis_area_code_20", "sample_source_name"],
            antibody_group_by_columns=["country_code_12", "sample_source_name"],
        )
    except DesignWeightError as e:
        print(e)  # functional
    return df


def join_and_process_lookups(
    household_level_populations_df: DataFrame,
    master_sample_df: DataFrame,
    old_sample_df: DataFrame,
    new_sample_df: DataFrame,
    new_sample_source_name: str,
    postcode_lookup_df: DataFrame,
    country_lookup_df: DataFrame,
    lsoa_cis_lookup_df: DataFrame,
    first_run: bool,
) -> DataFrame:
    """
    Add data from the additional lookup files to main dataset
    """
    master_sample_df = clean_postcode(master_sample_df, "postcode").withColumn(
        "ons_household_id", F.regexp_replace(F.col("ons_household_id"), r"\D", "")
    )

    postcode_lookup_df = clean_postcode(postcode_lookup_df, "postcode")

    old_sample_df = recode_columns(old_sample_df, new_sample_df, household_level_populations_df)
    old_sample_df = old_sample_df.withColumn(
        "date_sample_created", F.to_timestamp(F.lit("2021-11-30"), format="yyyy-MM-dd")
    )
    new_sample_df = assign_filename_column(new_sample_df, "sample_source_file")
    new_sample_df = new_sample_df.withColumn(
        "date_sample_created", F.to_timestamp(F.lit("2021-12-06"), format="yyyy-MM-dd")
    )
    new_sample_df = new_sample_df.withColumn("sample_source_name", F.lit(new_sample_source_name))

    if first_run:
        old_sample_df = assign_filename_column(old_sample_df, "sample_source_file")
        df = union_multiple_tables([old_sample_df, new_sample_df])
    else:
        df = new_sample_df

    df = df.join(
        master_sample_df.select("ons_household_id", "postcode", "sample_type"),
        on="ons_household_id",
        how="left",
    )
    df = df.withColumn(
        "sample_addressbase_indicator", F.when(F.col("sample_type").isin(["AB", "AB-Trial"]), 1).otherwise(0)
    ).withColumn("batch_number", F.lit(1))

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
    if not first_run:
        df = union_multiple_tables([old_sample_df, df])

    return df


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


def swab_weight_wrapper(df: DataFrame, household_level_populations_df: DataFrame, cis_window: Window):
    """
    Wrapper function to calculate swab design weights
    """
    df = calculate_design_weight_swabs(
        df=df,
        household_level_populations_df=household_level_populations_df,
        column_name_to_assign="raw_design_weights_swab",
        sample_type_column="sample_new_previous",
        group_by_columns=["cis_area_code_20", "sample_new_previous"],
        barcode_column="ons_household_id",
        previous_design_weight_column="scaled_design_weight_swab_non_adjusted",
    )
    df = calculate_generic_design_weight_variables(
        df=df,
        design_weight_column="raw_design_weights_swab",
        num_eligible_household_column="number_eligible_household_sample",
        groupby_columns=["cis_area_code_20", "sample_new_previous"],
        test_type="swab",
        cis_window=cis_window,
    )
    df = calculate_combined_design_weight_swabs(
        df=df,
        design_weight_column="combined_design_weight_swab",
        num_households_column="number_of_households_by_cis_area",
        cis_window=cis_window,
    )
    return df


def antibody_weight_wrapper(df: DataFrame, cis_window: Window, scenario: str = "A"):
    if scenario in "AB":
        design_weight_column = "raw_design_weight_antibodies_ab"
        df = calculate_scenario_ab_antibody_design_weights(
            df=df,
            column_name_to_assign=design_weight_column,
            hh_design_weight_antibodies_column="scaled_design_weight_antibodies_non_adjusted",
            sample_new_previous_column="sample_new_previous",
            scaled_design_weight_swab_non_adjusted_column="scaled_design_weight_swab_non_adjusted",
        )
    elif scenario == "C":
        design_weight_column = "raw_design_weight_antibodies_c"
        df = calculate_scenario_c_antibody_design_weights(
            df=df,
            column_name_to_assign=design_weight_column,
            sample_new_previous_column="sample_new_previous",
            tranche_eligible_column="tranche_eligible_households",
            tranche_number_column="tranche",
            swab_design_weight_column="scaled_design_weight_swab_non_adjusted",
            tranche_factor_column="tranche_factor",
            previous_design_weight_column="scaled_design_weight_antibodies_non_adjusted",
        )
    df = calculate_generic_design_weight_variables(
        df=df,
        design_weight_column=design_weight_column,
        num_eligible_household_column="number_eligible_household_sample",
        groupby_columns=["cis_area_code_20", "sample_new_previous"],
        test_type="antibody",
        cis_window=cis_window,
    )
    return df


def calculate_design_weight_swabs(
    df: DataFrame,
    household_level_populations_df: DataFrame,
    column_name_to_assign: str,
    sample_type_column: str,
    group_by_columns: List[str],
    barcode_column: str,
    previous_design_weight_column: str,
):
    """
    Assign design weight for swabs sample by applying the ratio between number_of_households_population_by_cis and
    number_eligible_household_sample when the sample type is "new"
    """
    df = df.drop("number_of_households_by_cis_area").join(
        household_level_populations_df.select(
            "number_of_households_by_cis_area", "number_of_households_by_country", "cis_area_code_20"
        ).distinct(),
        on="cis_area_code_20",
        how="left",
    )
    df = assign_distinct_count_in_group(
        df,
        "number_eligible_household_sample",
        count_distinct_columns=[barcode_column],
        group_by_columns=group_by_columns,
    )
    df = df.withColumn(
        column_name_to_assign,
        F.when(
            F.col(sample_type_column) == "new",
            F.col("number_of_households_by_cis_area") / F.col("number_eligible_household_sample"),
        ).otherwise(F.col(previous_design_weight_column)),
    )
    return df


def calculate_generic_design_weight_variables(
    df: DataFrame,
    design_weight_column: str,
    num_eligible_household_column: str,
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
        F.col(num_eligible_household_column) / F.col(f"design_effect_weight_{test_type}"),
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


def calculate_combined_design_weight_swabs(
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
        "scaled_design_weight_swab_non_adjusted",
        F.col("scaling_factor_combined_design_weight_swab") * F.col(design_weight_column),
    )
    return df


class DesignWeightError(Exception):
    pass


def validate_design_weights(
    df: DataFrame,
    num_households_by_cis_column: str,
    num_households_by_country_column: str,
    swab_weight_column: str,
    antibody_weight_column: str,
    cis_area_column: str,
    country_column: str,
    swab_group_by_columns: List[str],
    antibody_group_by_columns: List[str],
):
    """
    Validate the derived design weights by checking 4 conditions are true:
    - design weights add to household population total
    - no design weights are negative
    - no design weights are missing
    - design weights consistent by group
    """
    cis_area_window = Window.partitionBy(cis_area_column)
    country_window = Window.partitionBy(country_column)

    df = df.withColumn(
        "SWAB_DESIGN_WEIGHT_SUM_CHECK_FAILED",
        F.when(F.sum(swab_weight_column).over(cis_area_window) == F.col(num_households_by_cis_column), False).otherwise(
            True
        ),
    )
    df = df.withColumn(
        "ANTIBODY_DESIGN_WEIGHT_SUM_CHECK_FAILED",
        F.when(
            F.sum(antibody_weight_column).over(country_window) == F.col(num_households_by_country_column), False
        ).otherwise(True),
    )
    swab_design_weights_sum_to_population = (
        True if df.filter(F.col("SWAB_DESIGN_WEIGHT_SUM_CHECK_FAILED")).count() == 0 else False
    )
    antibody_design_weights_sum_to_population = (
        True if df.filter(F.col("ANTIBODY_DESIGN_WEIGHT_SUM_CHECK_FAILED")).count() == 0 else False
    )

    positive_design_weights = df.filter(F.least(swab_weight_column, antibody_weight_column) < 0).count()
    null_design_weights = df.filter(F.col(swab_weight_column).isNull() | F.col(antibody_weight_column).isNull()).count()

    df = assign_distinct_count_in_group(
        df, "SWAB_DISTINCT_DESIGN_WEIGHT_BY_GROUP", [swab_weight_column], swab_group_by_columns
    )
    df = assign_distinct_count_in_group(
        df, "ANTIBODY_DISTINCT_DESIGN_WEIGHT_BY_GROUP", [swab_weight_column], antibody_group_by_columns
    )
    swab_design_weights_inconsistent_within_group = (
        False if df.filter((F.col("SWAB_DISTINCT_DESIGN_WEIGHT_BY_GROUP") > 1)).count() == 0 else True
    )
    antibody_design_weights_inconsistent_within_group = (
        False if df.filter((F.col("ANTIBODY_DISTINCT_DESIGN_WEIGHT_BY_GROUP") > 1)).count() == 0 else True
    )

    df.drop(
        "SWAB_DESIGN_WEIGHT_SUM_CHECK_FAILED",
        "ANTIBODY_DESIGN_WEIGHT_SUM_CHECK_FAILED",
        "SWAB_DISTINCT_DESIGN_WEIGHT_BY_GROUP",
        "ANTIBODY_DISTINCT_DESIGN_WEIGHT_BY_GROUP",
    )
    error_string = ""
    if not antibody_design_weights_sum_to_population:
        error_string += "Antibody design weights do not sum to country population totals.\n"
    if not swab_design_weights_sum_to_population:
        error_string += "Swab design weights do not sum to cis area population totals.\n"
    if positive_design_weights > 0:
        error_string += f"{positive_design_weights} records have negative design weights.\n"
    if null_design_weights > 0:
        error_string += f"There are {null_design_weights} records with null swab or antibody design weights.\n"
    if swab_design_weights_inconsistent_within_group:
        error_string += "Swab design weights are not consistent within CIS area population groups.\n"
    if antibody_design_weights_inconsistent_within_group:
        error_string += "Antibody design weights are not consistent within country population groups.\n"
    if error_string:
        raise DesignWeightError(error_string)


def chose_scenario_of_design_weight_for_antibody_different_household(
    df: DataFrame,
    eligibility_pct_column: str,
    tranche_eligible_indicator: str,
    n_eligible_hh_tranche_by_strata_column,
    n_sampled_hh_tranche_by_strata_column,
) -> Union[str, None]:
    """
    Decide what scenario to use for calculation of the design weights
    for antibodies for different households
    Parameters
    ----------
    df
    tranche_eligible_indicator
    household_samples_dataframe
    n_eligible_hh_tranche_by_strata_column
    n_sampled_hh_tranche_by_strata_column
    """
    if tranche_eligible_indicator not in df.columns:
        return "A"
    df = df.withColumn(
        eligibility_pct_column,
        F.when(
            F.col(n_eligible_hh_tranche_by_strata_column).isNull()
            & F.col(n_sampled_hh_tranche_by_strata_column).isNull(),
            0,
        )
        .when(
            F.col(n_eligible_hh_tranche_by_strata_column).isNotNull()
            & F.col(n_sampled_hh_tranche_by_strata_column).isNotNull()
            & (F.col(n_sampled_hh_tranche_by_strata_column) > 0),
            (
                100
                * (F.col(n_eligible_hh_tranche_by_strata_column) - F.col(n_sampled_hh_tranche_by_strata_column))
                / F.col(n_sampled_hh_tranche_by_strata_column)
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


def calculate_scenario_ab_antibody_design_weights(
    df: DataFrame,
    column_name_to_assign: str,
    hh_design_weight_antibodies_column: str,
    sample_new_previous_column: str,
    scaled_design_weight_swab_non_adjusted_column: str,
) -> DataFrame:
    """ """

    df = df.withColumn(
        column_name_to_assign,
        F.when(
            F.col(sample_new_previous_column) == "previous",
            F.col(hh_design_weight_antibodies_column),
        ).otherwise(F.col(scaled_design_weight_swab_non_adjusted_column)),
    )
    return df


# 1169
def calculate_scenario_c_antibody_design_weights(
    df: DataFrame,
    column_name_to_assign: str,
    sample_new_previous_column: str,
    tranche_eligible_column: str,
    tranche_number_column: str,
    swab_design_weight_column: str,
    tranche_factor_column: str,
    previous_design_weight_column: str,
) -> DataFrame:
    """
    1 step: for cases with sample_new_previous = "previous" AND tranche_eligible_households=yes(1)
    AND  tranche_number_indicator = max value, calculate raw design weight antibodies by using
    tranche_factor
    2 step: for cases with sample_new_previous = "previous";  tranche_number_indicator != max value;
    tranche_ eligible_households != yes(1), calculate the  raw design weight antibodies by using
    previous design_weight antibodies
    3 step: for cases with sample_new_previous = new, calculate  raw design weights antibodies
    by using  design weights for swab
    """
    df = df.withColumn("MAX_TRANCHE_NUMBER", F.max(tranche_number_column).over(Window.partitionBy(F.lit(0))))

    df = df.withColumn(
        column_name_to_assign,
        F.when(
            (F.col(sample_new_previous_column) == "previous")
            & (F.col(tranche_eligible_column) == "Yes")
            & (F.col(tranche_number_column) == F.col("MAX_TRANCHE_NUMBER")),
            F.col(swab_design_weight_column) * F.col(tranche_factor_column),
        ).otherwise(
            F.when(
                F.col(sample_new_previous_column) == "previous",
                F.col(previous_design_weight_column),
            ).otherwise(F.col(swab_design_weight_column))
        ),
    )
    return df.drop("MAX_TRANCHE_NUMBER")


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
    df = df.withColumn("carry_forward_design_weight_antibodies", F.col(scenario_carry_forward_lookup[scenario]))
    df = df.withColumn(
        "sum_carry_forward_design_weight_antibodies", F.sum("carry_forward_design_weight_antibodies").over(window)
    )
    df = df.withColumn(
        "scaling_factor_carry_forward_design_weight_antibodies",
        F.col(household_population_column) / F.col("sum_carry_forward_design_weight_antibodies"),
    )
    df = df.withColumn(
        "scaled_design_weight_antibodies_non_adjusted",
        F.col("scaling_factor_carry_forward_design_weight_antibodies")
        * F.col("carry_forward_design_weight_antibodies"),
    )
    return df
