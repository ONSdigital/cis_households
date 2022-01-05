from chispa import assert_df_equality
from pyspark.sql import functions as F

from cishouseholds.weights.pre_calibration import adjust_design_weight_by_non_response_factor


def test_adjust_design_weight_by_non_response_factor(spark_session):
    schema_expected = """response_indicator integer,
                        household_level_designweight_swab double,
                        household_level_designweight_antibodies double,
                        bounded_non_response_factor double,
                        household_level_designweight_adjusted_swab double,
                        household_level_designweight_adjusted_antibodies double"""
    data_expected_df = [
        (1, 1.3, 1.6, 1.8, 2.3, 2.9),
        (1, 1.7, 1.4, 0.6, 1.0, 0.8),
        (0, 1.3, 1.6, 1.8, None, None),
    ]

    df_expected = spark_session.createDataFrame(data_expected_df, schema=schema_expected)

    df_input = df_expected.drop(
        "household_level_designweight_adjusted_swab", "household_level_designweight_adjusted_antibodies"
    )

    df_output = adjust_design_weight_by_non_response_factor(df_input)

    assert_df_equality(df_output, df_expected, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)
