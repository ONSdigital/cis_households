from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import functions as F

from cishouseholds.pipeline.post_merge_processing import impute_key_demographics


def test_impute_key_demographics(spark_session):
    """Test that high level imputation fills all missing values."""
    schema = """participant_id string, ons_household_id string, gor9d string,
                white boolean, sex string, date_of_birth string, visit_datetime string"""
    input_data = [
        ("A", "A-A", "gor9d-1", True, "Female", "1990-01-01", "1990-01-01"),
        ("A", "A-A", "gor9d-1", True, "Female", None, "1990-01-02"),  # Fill forward
        ("A", "A-B", "gor9d-1", None, None, "1990-01-01", "1990-01-01"),  # Impute
    ]
    input_df = spark_session.createDataFrame(input_data, schema=schema)
    expected_data = [
        ("A", "A-A", "gor9d-1", True, "Female", "1990-01-01", "1990-01-01"),
        ("A", "A-A", "gor9d-1", True, "Female", "1990-01-01", "1990-01-02"),
        ("A", "A-B", "gor9d-1", True, "Female", "1990-01-01", "1990-01-01"),
    ]
    expected_df = spark_session.createDataFrame(expected_data, schema=schema)

    output_df = impute_key_demographics(input_df)

    comparison_columns = ["participant_id", "ons_household_id", "white", "sex", "date_of_birth", "visit_datetime"]
    assert_df_equality(
        output_df.select(*comparison_columns),
        expected_df.select(*comparison_columns),
        ignore_column_order=True,
        ignore_row_order=True,
    )

    for demographic_variable in ["white", "sex", "date_of_birth"]:
        assert output_df.where(F.col(demographic_variable).isNull()).count() == 0
