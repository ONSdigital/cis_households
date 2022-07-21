import re
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window

from cishouseholds.derive import assign_count_by_group
from cishouseholds.derive import assign_distinct_count_in_group
from cishouseholds.derive import assign_filename_column
from cishouseholds.edit import clean_postcode
from cishouseholds.merge import union_multiple_tables
from cishouseholds.weights.derive import assign_sample_new_previous
from cishouseholds.weights.derive import assign_tranche_factor
from cishouseholds.weights.edit import fill_nulls
from cishouseholds.weights.edit import join_on_existing


def calculate_design_weights(
    household_level_populations_df: DataFrame,
    master_sample_df: DataFrame,
    old_sample_df: DataFrame,
    new_sample_df: DataFrame,
    new_sample_source_name: str,
    tranche_df: DataFrame,
    postcode_lookup_df: DataFrame,
    country_lookup_df: DataFrame,
    lsoa_cis_lookup_df: DataFrame,
    tranche_strata_columns: List[str],
    first_run: bool,
):
    """
    Wrapper for calling each of the functions necessary to generate the design weights

    Parameters
    -----------

    household_level_populations
        dataframe containing information about each individual household
    master_sample_df
        lookup data containing additional information for given participants
    old_sample_df
        previously processed sample data
    new_sample_df
        to be processed sample data
    new_sample_source_name
    tranche_df
        optional table containing information about a tranche of survey participants
    postcode_lookup_df
    country_lookup_df
    lsoa_cis_lookup_df
    first_run
        boolean flag to denote if this is the first run of this dataset.
        On first run dataset will require certain processes which are later not necessary
        once some data is tabulated
    """
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

    tranche_provided = tranche_df is not None
    if tranche_provided:
        tranche_df = assign_filename_column(tranche_df, "tranche_source_file")
        tranche_df = tranche_df.withColumn("tranche_eligible_households", F.lit("Yes"))

        df = join_on_existing(df=df, df_to_join=tranche_df, on=["ons_household_id"])
        df = df.withColumn(
            "tranche_eligible_households",
            F.when(F.col("tranche_eligible_households").isNull(), "No").otherwise(F.col("tranche_eligible_households")),
        )
        df = assign_tranche_factor(
            df=df,
            column_name_to_assign="tranche_factor",
            household_id_column="ons_household_id",
            tranche_column="tranche_number_indicator",
            elibility_column="tranche_eligible_households",
            strata_columns=tranche_strata_columns,
        )
    else:
        df = df.withColumn("tranche_eligible_households", F.lit("No"))

    cis_area_window = Window.partitionBy("cis_area_code_20")
    df = calculate_scaled_swab_design_weights(
        df=df,
        column_name_to_assign="scaled_design_weight_swab_non_adjusted",
        household_level_populations_df=household_level_populations_df,
        cis_window=cis_area_window,
    )

    higher_eligibility = df.where(F.col("tranche_factor") > 1.0).count() > 0
    new_migration_to_antibody = higher_eligibility and tranche_provided
    df = calculate_scaled_antibody_design_weights(
        df,
        "scaled_design_weight_antibodies_non_adjusted",
        "scaled_design_weight_swab_non_adjusted",
        cis_area_window,
        new_migration_to_antibody,
    )

    validate_design_weights(
        df=df,
        num_households_by_cis_column="number_of_households_by_cis_area",
        num_households_by_country_column="number_of_households_by_country",
        swab_desing_weight_column="scaled_design_weight_swab_non_adjusted",
        antibody_design_weight_column="scaled_design_weight_antibodies_non_adjusted",
        cis_area_column="cis_area_code_20",
        country_column="country_code_12",
    )
    return df


def calculate_scaled_antibody_design_weights(
    df: DataFrame,
    column_name_to_assign: str,
    scaled_swab_design_weight_column: str,
    cis_area_window: Window,
    new_migration_to_antibody: bool,
):
    design_weight_column = "antibody_design_weight"
    if new_migration_to_antibody:
        df = calculate_scenario_c_raw_antibody_design_weights(
            df=df,
            column_name_to_assign="raw_antibody_design_weight",
            sample_new_previous_column="sample_new_previous",
            tranche_eligible_column="tranche_eligible_households",
            tranche_number_column="tranche_number_indicator",
            scaled_swab_design_weight_column=scaled_swab_design_weight_column,
            tranche_factor_column="tranche_factor",
            previous_design_weight_column="scaled_design_weight_antibodies_non_adjusted",
        )
        df = calculate_combined_design_weights(
            df=df,
            column_name_to_assign=design_weight_column,
            design_weight_column="raw_antibody_design_weight",
            eligible_household_count_column="number_eligible_household_sample",
            groupby_columns=["cis_area_code_20", "sample_new_previous"],
            cis_window=cis_area_window,
        )

    else:
        df = calculate_scenario_ab_antibody_design_weights(
            df=df,
            column_name_to_assign=design_weight_column,
            hh_design_weight_antibodies_column="scaled_design_weight_antibodies_non_adjusted",
            sample_new_previous_column="sample_new_previous",
            scaled_swab_design_weight_column=scaled_swab_design_weight_column,
        )

    df = scale_antibody_design_weights(
        df=df,
        column_name_to_assign=column_name_to_assign,
        design_weight_column_to_scale=design_weight_column,
        groupby_column="cis_area_code_20",
        household_population_column="number_of_households_by_cis_area",
    )

    return df.drop(design_weight_column, "raw_antibody_design_weight")


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

    # Temporary fix for where postcode is missing or incomplete and can't be used to get country_code_12
    df = derive_country_code(df, "country_code_12", "region_code")

    df = df.join(
        country_lookup_df.select("country_code_12", "country_name_12").distinct(), on="country_code_12", how="left"
    )
    if not first_run:
        df = union_multiple_tables([old_sample_df, df])

    return df


def derive_country_code(df, column_name_to_assign: str, region_code_column: str):
    """Derive country code from region code starting character."""
    df = df.withColumn(
        column_name_to_assign,
        F.when(F.col(region_code_column).startswith("E"), "E92000001")
        .when(F.col(region_code_column).startswith("N"), "N92000002")
        .when(F.col(region_code_column).startswith("S"), "S92000003")
        .when(F.col(region_code_column).startswith("W"), "W92000004")
        .when(F.col(region_code_column).startswith("L"), "L93000001")
        .when(F.col(region_code_column).startswith("M"), "M83000003"),
    )
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


def calculate_scaled_swab_design_weights(
    df: DataFrame, column_name_to_assign: str, household_level_populations_df: DataFrame, cis_window: Window
):
    """
    Wrapper function to calculate swab design weights
    """
    df = calculate_raw_design_weight_swabs(
        df=df,
        household_level_populations_df=household_level_populations_df,
        column_name_to_assign="raw_design_weights_swab",
        sample_type_column="sample_new_previous",
        group_by_columns=["cis_area_code_20", "sample_new_previous"],
        barcode_column="ons_household_id",
        previous_design_weight_column=column_name_to_assign,
    )

    df = calculate_combined_design_weights(
        df=df,
        column_name_to_assign="combined_antibody_design_weight",
        design_weight_column="raw_design_weights_swab",
        eligible_household_count_column="number_eligible_household_sample",
        groupby_columns=["cis_area_code_20", "sample_new_previous"],
        cis_window=cis_window,
    )
    df = scale_swab_design_weight(
        df=df,
        column_name_to_assign=column_name_to_assign,
        design_weight_column="combined_antibody_design_weight",
        household_count_column="number_of_households_by_cis_area",
        cis_window=cis_window,
    )
    df = df.withColumn(
        column_name_to_assign,
        F.col(column_name_to_assign),
    )
    return df.drop("combined_antibody_design_weight")


def calculate_raw_design_weight_swabs(
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


def calculate_combined_design_weights(
    df: DataFrame,
    column_name_to_assign: str,
    design_weight_column: str,
    eligible_household_count_column: str,
    groupby_columns: List[str],
    cis_window: Window,
) -> DataFrame:
    """
    Calculate combined design weight for either swab or antibody.
    """
    window = Window.partitionBy(*groupby_columns)
    standard_deviation_raw_design_weight = F.stddev(design_weight_column).over(window)
    mean_raw_design_weight = F.mean(design_weight_column).over(window)

    coefficient_of_variation = standard_deviation_raw_design_weight / mean_raw_design_weight
    design_effect_weight = 1 + F.pow(coefficient_of_variation, 2)

    effective_sample_size = fill_nulls((F.col(eligible_household_count_column) / design_effect_weight), fill_value=0)
    sum_effective_sample_size = F.sum(effective_sample_size).over(cis_window)

    combining_factor = effective_sample_size / sum_effective_sample_size

    return df.withColumn(column_name_to_assign, combining_factor * F.col(design_weight_column))


def scale_swab_design_weight(
    df: DataFrame,
    column_name_to_assign: str,
    design_weight_column: str,
    household_count_column: str,
    cis_window: Window,
) -> DataFrame:
    """
    Apply logic to derive overall design weights
    """
    sum_combined_design_weight = F.sum(design_weight_column).over(cis_window)
    scaling_factor = F.col(household_count_column) / sum_combined_design_weight
    df = df.withColumn(column_name_to_assign, (scaling_factor * F.col(design_weight_column)).cast(DecimalType(38, 20)))
    return df


class DesignWeightError(Exception):
    pass


def validate_design_weights(
    df: DataFrame,
    num_households_by_cis_column: str,
    num_households_by_country_column: str,
    swab_desing_weight_column: str,
    antibody_design_weight_column: str,
    cis_area_column: str,
    country_column: str,
    rounding_value: float = 0,
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
        F.when(
            F.round(F.sum(swab_desing_weight_column).over(cis_area_window), rounding_value)
            == F.col(num_households_by_cis_column),
            False,
        ).otherwise(True),
    )
    df = df.withColumn(
        "ANTIBODY_DESIGN_WEIGHT_SUM_CHECK_FAILED",
        F.when(
            F.round(F.sum(antibody_design_weight_column).over(country_window), rounding_value)
            == F.col(num_households_by_country_column),
            False,
        ).otherwise(True),
    )
    swab_design_weights_sum_to_population = (
        True if df.filter(F.col("SWAB_DESIGN_WEIGHT_SUM_CHECK_FAILED")).count() == 0 else False
    )
    antibody_design_weights_sum_to_population = (
        True if df.filter(F.col("ANTIBODY_DESIGN_WEIGHT_SUM_CHECK_FAILED")).count() == 0 else False
    )

    negative_design_weights = df.filter(F.least(swab_desing_weight_column, antibody_design_weight_column) < 0).count()
    null_design_weights = df.filter(
        F.col(swab_desing_weight_column).isNull() | F.col(antibody_design_weight_column).isNull()
    ).count()

    df.drop(
        "SWAB_DESIGN_WEIGHT_SUM_CHECK_FAILED",
        "ANTIBODY_DESIGN_WEIGHT_SUM_CHECK_FAILED",
    )
    error_string = ""

    if not antibody_design_weights_sum_to_population:
        error_string += "\n- Antibody design weights do not sum to country population totals."
    if not swab_design_weights_sum_to_population:
        error_string += "\n- Swab design weights do not sum to cis area population totals."
    if negative_design_weights > 0:
        error_string += f"\n- {negative_design_weights} records have negative design weights."
    if null_design_weights > 0:
        error_string += f"\n- There are {null_design_weights} records with null swab or antibody design weights."
    if error_string:
        raise DesignWeightError(error_string + "\n")


def calculate_scenario_ab_antibody_design_weights(
    df: DataFrame,
    column_name_to_assign: str,
    hh_design_weight_antibodies_column: str,
    sample_new_previous_column: str,
    scaled_swab_design_weight_column: str,
) -> DataFrame:
    """
    Use the sample_new_previous column value to either select the hh_design_weight_antibodies_column or
    scaled_design_weight_swab_non_adjusted_column as the scenario A/B design weight value.
    """

    df = df.withColumn(
        column_name_to_assign,
        F.when(
            F.col(sample_new_previous_column) == "previous",
            F.col(hh_design_weight_antibodies_column),
        ).otherwise(F.col(scaled_swab_design_weight_column)),
    )
    return df


def calculate_scenario_c_raw_antibody_design_weights(
    df: DataFrame,
    column_name_to_assign: str,
    sample_new_previous_column: str,
    tranche_eligible_column: str,
    tranche_number_column: str,
    scaled_swab_design_weight_column: str,
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
            F.col(scaled_swab_design_weight_column) * F.col(tranche_factor_column),
        ).otherwise(
            F.when(
                F.col(sample_new_previous_column) == "previous",
                F.col(previous_design_weight_column),
            ).otherwise(F.col(scaled_swab_design_weight_column))
        ),
    )
    return df.drop("MAX_TRANCHE_NUMBER")


def scale_antibody_design_weights(
    df: DataFrame,
    column_name_to_assign: str,
    design_weight_column_to_scale: str,
    groupby_column: str,
    household_population_column: str,
):
    """
    Use scenario lookup to apply dependent function to carry forward design weights variable
    to current dataframe

    Parameters
    ----------
    df
    scenario
        A,B,C depending on previous conditions decides which logic to perform
    groupby_column
        column name whose values are used to subdivide the dataset
    household_population_column
    """
    window = Window.partitionBy(groupby_column)
    sum_carry_forward_design_weight = F.sum(F.col(design_weight_column_to_scale)).over(window)
    scaling_factor = F.col(household_population_column) / sum_carry_forward_design_weight

    df = df.withColumn(
        column_name_to_assign,
        (scaling_factor * F.col(design_weight_column_to_scale)).cast(DecimalType(38, 20)),
    )
    return df
