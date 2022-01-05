from chispa import assert_df_equality
from pyspark.sql import functions as F

from cishouseholds.weights.pre_calibration import adjusted_design_weights_to_population_totals


def test_adjusted_design_weights_to_population_totals(spark_session):
    schema_expected = """country_name_12 string,
                         response_indicator integer,
                         household_level_designweight_adjusted_swab double,
                         household_level_designweight_adjusted_antibodies double,
                         population_country_swab integer,
                         population_country_antibodies integer,
                         sum_adjusted_design_weight_swab double,
                         sum_adjusted_design_weight_antibodies double,
                         scaling_factor_adjusted_design_weight_swab double,
                         scaling_factor_adjusted_design_weight_antibodies double,
                         scaled_design_weight_adjusted_swab double,
                         scaled_design_weight_adjusted_antibodies double
                         """
    data_expected_df = [
        ("England", 1, 1.2, 1.4, 250, 350, 5.8, 5.4, 43.1, 64.8, 51.7, 90.7),
        ("England", 1, 1.5, 1.2, 250, 350, 5.8, 5.4, 43.1, 64.8, 64.7, 77.8),
        ("England", 1, 1.8, 1.6, 250, 350, 5.8, 5.4, 43.1, 64.8, 77.6, 103.7),
        ("England", 1, 0.7, 0.4, 250, 350, 5.8, 5.4, 43.1, 64.8, 30.2, 25.9),
        ("England", 1, 0.6, 0.8, 250, 350, 5.8, 5.4, 43.1, 64.8, 25.9, 51.8),
        ("England", 0, None, None, 250, 350, None, None, None, None, None, None),
        ("England", 0, None, None, 250, 350, None, None, None, None, None, None),
    ]

    df_expected = spark_session.createDataFrame(data_expected_df, schema=schema_expected)

    df_input = df_expected.drop("sum_adjusted_design_weight_swab double", "sum_adjusted_design_weight_antibody double")

    df_output = adjusted_design_weights_to_population_totals(df_input)

    assert_df_equality(df_output, df_expected, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)
