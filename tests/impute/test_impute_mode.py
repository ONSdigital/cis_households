from pyspark.sql import functions as F

from chispa import assert_df_equality

from cishouseholds.impute import calculate_imputation_from_mode


def test_impute_mode(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            # No missing values
            ("0", "A", None),
            ("0", "B", None),
            # One option
            ("1", "A", None),
            ("1", None, "A"),
            # A and B but A is the most common
            ("3", "A", None),
            ("3", "A", None),
            ("3", "B", None),
            ("3", None, "A"),
            # Tie results in no imputation
            ("4", "A", None),
            ("4", "B", None),
            ("4", None, None),
            # Don't impute as Null, when Null is most common
            ("5", "A", None),
            ("5", None, "A"),
            ("5", None, "A"),
        ],
        schema="group_id string, value string, imputed_value string",
    ).withColumn("unique_id", F.monotonically_increasing_id())
    df_input = expected_df.drop("imputed_value")

    actual_df = calculate_imputation_from_mode(df_input, "imputed_value", "value", "group_id")
    assert_df_equality(actual_df, expected_df, ignore_row_order=True, ignore_column_order=True)
