from typing import List
from typing import Union

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from cishouseholds.merge import union_multiple_tables
from cishouseholds.weights.derive import assign_sample_new_previous
from cishouseholds.weights.derive import assign_tranche_factor
from cishouseholds.weights.derive import get_matches
from cishouseholds.weights.edit import clean_df
from cishouseholds.weights.edit import join_on_existing
from cishouseholds.weights.edit import null_to_value
from cishouseholds.weights.edit import update_data
from cishouseholds.weights.extract import prepare_auxillary_data

# from cishouseholds.weights.extract import load_auxillary_data

# notes:
# validation checks relate to final dweights for entire column for both datasets (each hh must have a dweight)


def generate_weights(auxillary_dfs):
    auxillary_dfs = prepare_auxillary_data(auxillary_dfs)

    # initialise lookup dataframes
    household_info_df = household_dweights(
        auxillary_dfs["address_lookup"],
        auxillary_dfs["nspl_lookup"],
        auxillary_dfs["cis20cd_lookup"],
        auxillary_dfs["country_lookup"],
    )

    # 1164
    df = get_matches(
        old_sample_df=auxillary_dfs["old"],
        new_sample_df=auxillary_dfs["new"],
        selection_columns=["lower_super_output_area_code_11", "cis_area_code_20"],
        barcode_column="ons_household_id",
    )

    # update and clean sample df's
    df = update_data(df, auxillary_dfs)
    df = clean_df(df)
    old_df = clean_df(auxillary_dfs["old"])
    df = union_multiple_tables(tables=[df, old_df])

    # transform sample files
    df = assign_sample_new_previous(df, "sample_new_previous", "date_sample_created", "batch_number")
    tranche_df = auxillary_dfs["tranche"].withColumn("TRANCHE_BARCODE_REF", F.col("ons_household_id"))

    # df = df.join(tranche_df, on="ons_household_id", how="leftouter").drop("UAC")
    df = join_on_existing(df=df, df_to_join=tranche_df, on=["ons_household_id"]).drop("UAC")

    df = assign_tranche_factor(
        df=df,
        column_name_to_assign="tranche_factor",
        barcode_column="ons_household_id",
        barcode_ref_column="TRANCHE_BARCODE_REF",
        tranche_column="tranche",
        group_by_columns=["cis_area_code_20", "enrolement_date"],
    )

    cis_window = Window.partitionBy("cis_area_code_20")
    df = swab_weight_wrapper(df=df, household_info_df=household_info_df, cis_window=cis_window)
    df = antibody_weight_wrapper(df=df, cis_window=cis_window)

    scenario_string = chose_scenario_of_dweight_for_antibody_different_household(
        df=df,
        tranche_eligible_indicator="tranche_eligible_households",
        eligibility_pct_column="eligibility_pct",
        n_eligible_hh_tranche_bystrata_column="number_eligible_households_tranche_bystrata_enrolment",
        n_sampled_hh_tranche_bystrata_column="number_sampled_households_tranche_bystrata_enrolment",
    )

    df = validate_design_weights(
        df=df,
        column_name_to_assign="validated_design_weights",
        num_households_column="number_of_households_population_by_cis",
        window=cis_window,
    )
    df = carry_forward_design_weights(
        df=df,
        scenario=scenario_string,
        groupby_column="cis_area_code_20",
        household_population_column="number_of_households_population_by_cis",
    )
    return df
    # df.toPandas().to_csv("full_out.csv", index=False)


# 1163
# necessary columns:
# - postcode
# - lower_super_output_area_code_11
# - country_code_12
# - cis_area_code_20
# - unique_property_reference_code
def household_dweights(
    df_address_base: DataFrame, df_nspl: DataFrame, df_cis20cd: DataFrame, df_country: DataFrame
) -> DataFrame:
    """
    Steps:
    1. merge address base extract and NSPL by postcode
    2. merge the result of first step  into LSOAtoCis lookup, by lower_super_output_area_code_11
    3. merge the result of the second step with country name lookup by country_code
    4. calculate required population total columns on joined dataset

    df_address_base
        Dataframe with address base file
    df_nspl
        Dataframe linking postcodes and lower level output area.
    df_cis20cd
        Dataframe with cis20cd and interim id.
    """
    df = df_address_base.join(df_nspl, on="postcode", how="left").withColumn(
        "postcode", F.regexp_replace(F.col("postcode"), " ", "")
    )
    df = df.join(df_cis20cd, on="lower_super_output_area_code_11", how="left")
    df = df.join(df_country, on="country_code_12", how="left")

    area_window = Window.partitionBy("cis_area_code_20")
    df = df.withColumn(
        "number_of_households_population_by_cis",
        F.approx_count_distinct("unique_property_reference_code").over(area_window),
    )

    country_window = Window.partitionBy("country_code_12")
    df = df.withColumn(
        "number_of_households_population_by_country",
        F.approx_count_distinct("unique_property_reference_code").over(country_window),
    )
    return df


# Wrapper
def swab_weight_wrapper(df: DataFrame, household_info_df: DataFrame, cis_window: Window):
    """
    Wrapper function to calculate swab design weights
    """
    # Swab weights
    df = calculate_dweight_swabs(
        df=df,
        household_info_df=household_info_df,
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
        num_households_column="number_of_households_population_by_cis",
        cis_window=cis_window,
    )
    return df


# Wrapper
def antibody_weight_wrapper(df: DataFrame, cis_window: Window):
    # Antibody weights
    df = calculate_scenario_ab_antibody_dweights(
        df=df,
        column_name_to_assign="raw_design_weight_antibodies_ab",
        hh_dweight_antibodies_column="household_level_designweight_antibodies",
        sample_new_previous_column="sample_new_previous",
        scaled_dweight_swab_nonadjusted_column="scaled_design_weight_swab_nonadjusted",
    )
    df = calculate_scenario_c_antibody_dweights(
        df=df,
        column_name_to_assign="raw_design_weight_antibodies_c",
        sample_new_previous_column="sample_new_previous",
        tranche_eligible_column="tranche_eligible_households",
        tranche_num_column="tranche",
        design_weight_column="scaled_design_weight_swab_nonadjusted",
        tranche_factor_column="tranche_factor",
        household_dweight_column="household_level_designweight_antibodies",
    )
    df = calculate_generic_dweight_variables(
        df=df,
        design_weight_column="raw_design_weight_antibodies_c",
        num_eligible_hosusehold_column="number_eligible_household_sample",
        groupby_columns=["cis_area_code_20", "sample_new_previous"],
        test_type="antibody",
        cis_window=cis_window,
    )
    return df


# 1166
# necessary household df columns:
# - number_of_households_population_by_cis
# - number_of_households_population_by_country
# - cis_area_code_20

# necessary df columns:
# - cis_area_code_20
def calculate_dweight_swabs(
    df: DataFrame,
    household_info_df: DataFrame,
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
    df = df.join(
        household_info_df.select(
            "number_of_households_population_by_cis", "number_of_households_population_by_country", "cis_area_code_20"
        ),
        on="cis_area_code_20",
        how="left",
    )
    df = df.withColumn("number_eligible_household_sample", F.approx_count_distinct(barcode_column).over(window))
    df = df.withColumn(
        column_name_to_assign,
        F.when(
            F.col(sample_type_column) == "new",
            F.col("number_of_households_population_by_cis") / F.col("number_eligible_household_sample"),
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
    """ """
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


# 1166
def validate_design_weights(df: DataFrame, column_name_to_assign: str, num_households_column: str, window: Window):
    """
    Validate the derived design weights by checking 3 conditions are true:
    - design weights add to household population total
    - no design weights are negative
    - no design weights are missing
    - dweights consistent by cis area
    """
    columns = [col for col in df.columns if "weight" in col and list(df.select(col).dtypes[0])[1] == "double"]
    df = df.withColumn(column_name_to_assign, F.lit("True"))
    for col in columns:
        df = null_to_value(df, col)  # update nulls to 0
        df = df.withColumn(
            column_name_to_assign,
            F.when(
                F.sum(col).over(window) != F.col(num_households_column),
                "False",
            ).otherwise(F.col(column_name_to_assign)),
        )  # check 1
        df = df.withColumn(
            column_name_to_assign,
            F.when(F.approx_count_distinct(col).over(window) != 1, "False").otherwise(F.col(column_name_to_assign)),
        )

    df = df.withColumn("LEAST", F.least(*columns))
    df = df.withColumn(
        column_name_to_assign, F.when(F.col("LEAST") <= 0, "False").otherwise(F.col(column_name_to_assign))
    )  # flag missing

    return df.drop("LEAST")


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
    if tranche_eligible_indicator not in df.columns:  # TODO: not in household_samples_dataframe?
        return "A"
    else:
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
            F.col(hh_dweight_antibodies_column).isNotNull(),
            F.when(F.col(sample_new_previous_column) == "previous", F.col(hh_dweight_antibodies_column)).otherwise(
                F.col(scaled_dweight_swab_nonadjusted_column)
            ),
        ).otherwise(None),
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


# 1170
# necessary columns:
# - raw_design_weight_antibodies_ab OR
# - combined_design_weight_antibody
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


# if __name__ == "__main__":
#     try:
#         generate_weights(load_auxillary_data())
#     except Exception as e:
#         pass
# generate_weights()
