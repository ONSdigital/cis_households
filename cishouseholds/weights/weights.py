import re
from typing import Any
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from cishouseholds.weights.derive import assign_sample_new_previous
from cishouseholds.weights.derive import assign_tranche_factor
from cishouseholds.weights.derive import get_matches
from cishouseholds.weights.edit import clean_df
from cishouseholds.weights.edit import null_to_value
from cishouseholds.weights.edit import update_data
from cishouseholds.weights.extract import load_auxillary_data

from cishouseholds.derive import assign_named_buckets
from cishouseholds.derive import assign_proportion_column
from cishouseholds.edit import update_column_values_from_map
from cishouseholds.merge import union_multiple_tables


def generate_weights():
    # initialise all dataframes in dictionary
    auxillary_dfs = load_auxillary_data()

    # initialise lookup dataframes
    household_info_df = household_design_weights(
        auxillary_dfs["address_lookup"],
        auxillary_dfs["nspl_lookup"],
        auxillary_dfs["cis20cd_lookup"],
        auxillary_dfs["country_lookup"],
    )

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
    df = assign_sample_new_previous(df, "sample_new_previous", "date _sample_created", "batch_number")
    df = df.join(auxillary_dfs["tranche"], on="ons_household_id", how="outer").drop("UAC")
    df = assign_tranche_factor(df, "tranche_factor", "ons_household_id", ["cis_area_code_20", "enrolement_date"])

    df = calculate_dweight_swabs(
        df=df,
        household_info_df=household_info_df,
        column_name_to_assign="raw_design_weights_swab",
        sample_type_column="sample_new_previous",
        group_by_columns=["cis_area_code_20", "sample_new_previous"],
        barcode_column="ons_household_id",
        previous_dweight_column="household_level_designweight_swab",
    )
    df = calculate_overall_dweights(df, "raw_design_weights_swab", ["cis_area_code_20", "sample_new_previous"])


def household_design_weights(
    df_address_base: DataFrame, df_nspl: DataFrame, df_cis20cd: DataFrame, df_county: DataFrame
) -> DataFrame:
    """
    Join address base and nspl (National Statistics Postcode Lookup) to then left inner join
    lsoa (Lower Level Output Area) to get household count.
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
    df = df.join(df_county, on="country_code_12")

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
        how="outer",
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


def calculate_overall_dweights(df: DataFrame, design_weight_column: str, groupby_columns: List[str]) -> DataFrame:
    """
    Apply logic to derive overall design weights
    """
    window = Window.partitionBy(*groupby_columns)
    df = df.withColumn("sum_raw_design_weight_swab_cis", F.sum(design_weight_column).over(window))
    df = df.withColumn("standard_deviation_raw_design_weight_swab", F.stddev(design_weight_column).over(window))
    df = df.withColumn("mean_raw_design_weight_swab", F.mean(design_weight_column).over(window))

    df = df.withColumn(
        "coefficient_variation_design_weight_swab",
        (F.col("standard_deviation_raw_design_weight_swab") / F.col("mean_raw_design_weight_swab")),
    )
    df = df.withColumn("design_effect_weight_swab", 1 + F.pow(F.col("coefficient_variation_design_weight_swab"), 2))
    df = df.withColumn(
        "effective_sample_size_design_weight_swab",
        F.col("number_eligible_household_sample") / F.col("design_effect_weight_swab"),
    )
    df = null_to_value(df, "effective_sample_size_design_weight_swab")

    cis_window = Window.partitionBy(groupby_columns[0])
    df = df.withColumn(
        "sum_effective_sample_size_design_weight_swab",
        F.sum("effective_sample_size_design_weight_swab").over(cis_window),
    )
    df = df.withColumn(
        "combining_factor_design_weight_swab",
        F.col("effective_sample_size_design_weight_swab") / F.col("sum_effective_sample_size_design_weight_swab"),
    )
    df = df.withColumn(
        "combined_design_weight_swab", F.col("combining_factor_design_weight_swab") * F.col(design_weight_column)
    )

    df = df.withColumn("sum_combined_design_weight_swab", F.sum("combined_design_weight_swab").over(cis_window))
    df = df.withColumn(
        "scaling_factor_combined_design_weight_swab",
        F.col("number_of_households_population_by_cis") / F.col("sum_combined_design_weight_swab"),
    )
    df = df.withColumn(
        "scaled_design_weight_swab_nonadjusted",
        F.col("scaling_factor_combined_design_weight_swab") * F.col("combined_design_weight_swab"),
    )
    df = validate_design_weights(df, "validated_design_weights", cis_window)
    df.toPandas().to_csv("test_output4.csv", index=False)


def validate_design_weights(df: DataFrame, column_name_to_assign: str, window: Window):
    """
    Validate the derived design weights by checking 3 conditions are true:
    - design weights add to household population total
    - no design weights are negative
    - no design weights are missing
    """
    df = df.withColumn(
        column_name_to_assign,
        F.when(
            F.sum("scaled_design_weight_swab_nonadjusted").over(window)
            == F.col("number_of_households_population_by_cis"),
            "True",
        ).otherwise("False"),
    )  # check 1

    columns = [col for col in df.columns if "weight" in col and list(df.select(col).dtypes[0])[1] == "double"]

    for col in columns:
        df = null_to_value(df, col)  # update nulls to 0
        df = df.withColumn(
            column_name_to_assign,
            F.when(F.approx_count_distinct(col).over(window) != 1, "False").otherwise(F.col(column_name_to_assign)),
        )

    df = df.withColumn("LEAST", F.least(*columns))
    df = df.withColumn(
        column_name_to_assign, F.when(F.col("LEAST") <= 0, "False").otherwise(F.col(column_name_to_assign))
    )  # flag missing

    return df.drop("LEAST")


def carry_forward_dweights(df: DataFrame, scenario: str, window: Window, household_population_column: str):
    """
    Use scenario lookup to apply dependent function to carry forward design weights variable
    to current dataframe
    """
    scenario_carry_forward_lookup = {
        "A": "raw_design_weights_antibodies_ab",
        "B": "raw_design_weights_antibodies_ab",
        "C": "combined_design_weight_antibodies_c",
    }
    df = df.withColumn("carryforward_design_weight_antibodies", F.col(scenario_carry_forward_lookup[scenario]))
    df = df.withColumn(
        "sum_carryforward_design_weight_antibodies", F.sum("carryforward_design_weight_antibodies").over(window)
    )
    df = df.withColumn(
        "scalling_factor_carryforward_design_weight_antibodies",
        F.col(household_population_column) / F.col("sum_carryfoward_design_weight_antibodies"),
    )
    df = df.withColumn(
        "scaled_design_weight_antibodies_nonadjusted",
        F.col("scalling_factor_carryforward_design_weight_antibodies") * F.col("carryforward_design_weight_antibodies"),
    )


# generate_weights(resource_paths=resource_paths)
