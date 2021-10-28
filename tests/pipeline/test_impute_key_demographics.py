from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import functions as F

from cishouseholds.pipeline.post_merge_processing import impute_key_demographics


def test_impute_key_demographics(spark_session):
    """Test that high level imputation fills all missing values and reduces
    to one record per participant."""
    input_data = [
        ("A", "A-A", "1", True, "Female", "1990-01-01", "1990-01-01"),
        ("A", "A-A", "1", True, "Female", None, "1990-01-02"),  # Fill forward
        ("A", "A-B", "1", None, None, "1990-01-01", "1990-01-01"),  # Impute by mode
        ("B", "B-A", "2", False, "Female", None, "1990-01-01"),  # Impute by lookup
    ]
    input_df = spark_session.createDataFrame(
        input_data,
        schema="""ons_household_id string, participant_id string, gor9d string,
                white_group boolean, sex string, date_of_birth string, visit_datetime string""",
    )

    lookup_data = [("B-A", None, None, None, None, "1990-01-02", "method")]
    lookup_df = spark_session.createDataFrame(
        lookup_data,
        schema="""participant_id string, white_group boolean, white_group_imputation_method string,
        sex string, sex_imputation_method string, date_of_birth string, date_of_birth_imputation_method string""",
    )

    expected_data = [
        ("A-A", True, "Female", "1990-01-01", None, None, None),
        ("A-B", True, "Female", "1990-01-01", "impute_by_mode", "impute_by_distribution", None),
        ("B-A", False, "Female", "1990-01-02", None, None, "method"),
    ]
    expected_df = spark_session.createDataFrame(
        expected_data,
        schema="""participant_id string, white_group boolean, sex string, date_of_birth string,
                white_group_imputation_method string, sex_imputation_method string,
                date_of_birth_imputation_method string""",
    )

    comparison_columns = [
        "participant_id",
        "white_group",
        "sex",
        "date_of_birth",
        "white_group_imputation_method",
        "sex_imputation_method",
        "date_of_birth_imputation_method",
    ]
    output_df = impute_key_demographics(input_df, lookup_df, ["white_group", "sex", "date_of_birth"])

    assert_df_equality(
        output_df.select(*comparison_columns),
        expected_df,
        ignore_column_order=True,
        ignore_row_order=True,
    )

    for demographic_variable in ["white_group", "sex", "date_of_birth"]:
        assert output_df.where(F.col(demographic_variable).isNull()).count() == 0
