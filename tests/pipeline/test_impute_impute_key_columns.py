import os

import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import functions as F

from cishouseholds.pipeline.high_level_transformations import impute_key_columns


@pytest.mark.integration
def test_impute_key_columns(spark_session):
    """
    Test that high level imputation fills all missing values and reduces
    to one record per participant.

    Note that this doesn't include any cases to be imputed by KNN, as these are too memory heavy to be be run locally.
    """
    os.environ["deployment"] = "local"
    input_data = [
        # fmt: off
        ("A", "A-A", "1", "A", "g1", "3", "white", "Female", "1990-01-01", "1990-01-01"),
        ("A", "A-B", "1", "A", "g1", "3", None, None, "1990-01-01", "1990-01-01"),  # Impute by mode
        ("B", "B-A", "2", "B", "g1", "1", "other", "Female", None, "1990-01-01"),
        # Impute by lookup
        # fmt: on
    ]
    input_df = spark_session.createDataFrame(
        input_data,
        schema="""ons_household_id string, participant_id string, cis_area_code_20 string, region_code string, work_status_group string, people_in_household_count_group string,
                ethnicity_white string, sex string, date_of_birth string, visit_datetime string""",
    )

    lookup_data = [("B-A", None, None, None, None, "1990-01-02", "lookup_method")]
    lookup_df = spark_session.createDataFrame(
        lookup_data,
        schema="""participant_id string, ethnicity_white string, ethnicity_white_imputation_method string,
        sex string, sex_imputation_method string, date_of_birth string, date_of_birth_imputation_method string""",
    )

    expected_data = [
        # fmt: off
        ("A-A", "white", "Female", None, None, None),
        ("A-B", "white", "Female", "impute_by_mode", "impute_by_distribution", None),
        ("B-A", "other", "Female", None, None, "lookup_method"),
        # fmt: on
    ]
    expected_df = spark_session.createDataFrame(
        expected_data,
        schema="""participant_id string, ethnicity_white string, sex string, date_of_birth string,
                ethnicity_white_imputation_method string, sex_imputation_method string,
                date_of_birth_imputation_method string""",
    )

    imputation_columns = [
        "participant_id",
        "ethnicity_white",
        "sex",
        "date_of_birth",
        "ethnicity_white_imputation_method",
        "sex_imputation_method",
        "date_of_birth_imputation_method",
    ]
    output_df = impute_key_columns(input_df, lookup_df, log_directory="./").cache()

    assert_df_equality(
        output_df.select(*imputation_columns),
        expected_df.select(*imputation_columns),
        ignore_row_order=True,
    )

    for demographic_variable in ["ethnicity_white", "sex", "date_of_birth"]:
        assert output_df.where(F.col(demographic_variable).isNull()).count() == 0
