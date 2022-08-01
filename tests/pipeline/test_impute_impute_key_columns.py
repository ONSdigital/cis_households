import os

import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import functions as F

from cishouseholds.pipeline.high_level_transformations import impute_key_columns


@pytest.mark.integration
def test_impute_key_columns(spark_session):
    """Test that high level imputation fills all missing values and reduces
    to one record per participant."""
    os.environ["deployment"] = "local"
    input_data = [
        # fmt: off
        ("A", "A-A", "1", "A", "g1", "3", "white", "Female", "1990-01-01", "1990-01-01"),
        ("A", "A-A", "1", "B", "g1", "3", "white", "Female", None, "1990-01-02"),  # Fill forward
        ("A", "A-B", "1", "B", "g1", "3", None, None, "1990-01-01", "1990-01-01"),  # Impute by mode
        ("B", "B-A", "2", "B", "g1", "1", "other", "Female", None, "1990-01-01"),  # Impute by lookup
        ("C", "C-A", "2", "A", "g1", "1", None, "Female", "1990-01-01", "1990-01-01"),
        # Impute by KNN
        # fmt: on
    ]
    input_df = spark_session.createDataFrame(
        input_data,
        schema="""ons_household_id string, participant_id string, cis_area_code_20 string, region_code string, work_status_group string, people_in_household_count_group string,
                ethnicity_white string, sex string, date_of_birth string, visit_datetime string""",
    )

    lookup_data = [("B-A", None, None, None, None, "1990-01-02", "method")]
    lookup_df = spark_session.createDataFrame(
        lookup_data,
        schema="""participant_id string, ethnicity_white string, ethnicity_white_imputation_method string,
        sex string, sex_imputation_method string, date_of_birth string, date_of_birth_imputation_method string""",
    )

    expected_data = [
        # fmt: off
        ("A-A", "white", "Female", 1990, 1, None, None, None),
        ("A-B", "white", "Female", 1990, 1, "impute_by_mode", "impute_by_distribution", None),
        ("B-A", "other", "Female", 1990, 1, None, None, "method"),
        ("C-A", "other", "Female", 1990, 1, "impute_date_by_k_nearest_neighbours", None, None),
        # Impute by KNN
        # fmt: on
    ]
    expected_df = spark_session.createDataFrame(
        expected_data,
        schema="""participant_id string, ethnicity_white string, sex string, YEAR integer, MONTH integer,
                ethnicity_white_imputation_method string, sex_imputation_method string,
                date_of_birth_imputation_method string""",
    )

    value_columns = ["participant_id", "ethnicity_white", "sex", "YEAR", "MONTH"]
    method_columns = [
        "participant_id",
        "ethnicity_white_imputation_method",
        "sex_imputation_method",
        "date_of_birth_imputation_method",
    ]
    output_df = impute_key_columns(input_df, lookup_df, log_directory="./")
    print(output_df.dtypes)
    output_df = output_df.withColumn("YEAR", F.year("date_of_birth")).withColumn("MONTH", F.month("date_of_birth"))
    output_df.show()
    for columns in [value_columns, method_columns]:
        assert_df_equality(
            output_df.select(*columns),
            expected_df.select(*columns),
            ignore_row_order=True,
            ignore_column_order=True
        )

    for demographic_variable in ["ethnicity_white", "sex", "date_of_birth"]:
        assert output_df.where(F.col(demographic_variable).isNull()).count() == 0
