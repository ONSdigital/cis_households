import pytest

from cishouseholds.merge import union_multiple_tables
from cishouseholds.pipeline.pipeline_stages import union_dependent_cleaning
from cishouseholds.pipeline.pipeline_stages import union_dependent_derivations


@pytest.fixture(scope="session")
def union_transformed_output(
    responses_v0_survey_ETL_output,
    responses_v1_survey_ETL_output,
    responses_v2_survey_ETL_output,
    responses_digital_ETL_output,
):
    """
    Generate dummy survey responses digital.
    """
    df = union_multiple_tables(
        [
            responses_v0_survey_ETL_output,
            responses_v1_survey_ETL_output,
            responses_v2_survey_ETL_output,
            responses_digital_ETL_output,
        ]
    )
    df = union_dependent_cleaning(df)
    df = union_dependent_derivations(df)
    # df.orderBy("ons_household_id").drop("survey_response_source_file")

    df = df.drop("survey_response_source_file")
    return df


@pytest.mark.integration
def test_union_transformed_output(union_transformed_output, regression_test_df):
    regression_test_df(
        union_transformed_output, "visit_id", "processed_post_union_data_regression"
    )  # remove source file column, as it varies for our temp dummy data


@pytest.mark.integration
def test_union_transformed_schema(regression_test_df_schema, union_transformed_output):
    regression_test_df_schema(union_transformed_output, "processed_post_union_schema_regression")  # check
