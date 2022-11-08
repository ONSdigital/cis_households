import pytest

from cishouseholds.merge import union_multiple_tables
from cishouseholds.pipeline.high_level_transformations import fill_forwards_transformations
from cishouseholds.pipeline.pipeline_stages import union_dependent_cleaning
from cishouseholds.pipeline.pipeline_stages import union_dependent_derivations


@pytest.fixture
def union_dependent_transformations_df(
    responses_v0_survey_ETL_output,
    responses_v1_survey_ETL_output,
    responses_v2_survey_ETL_output,
    responses_digital_ETL_output,
):
    """
    Create union dependent transformations output dataframe
    """
    df = union_multiple_tables(
        [
            responses_v0_survey_ETL_output,
            responses_v1_survey_ETL_output,
            responses_v2_survey_ETL_output,
            responses_digital_ETL_output,
        ]
    )
    df = fill_forwards_transformations(df)
    df = union_dependent_cleaning(df)
    df = union_dependent_derivations(df)
    return df


@pytest.mark.regression
@pytest.mark.integration
def test_union_dependent_transformations_df(
    union_dependent_transformations_df,
    regression_test_df,
):
    regression_test_df(
        union_dependent_transformations_df.drop("survey_response_source_file"),
        "visit_id",
        "union_dependent_transformations",
    )  # remove source file column, as it varies for our temp dummy data


@pytest.mark.regression
@pytest.mark.integration
def test_union_dependent_transformations_schema(
    union_dependent_transformations_df,
    regression_test_df_schema,
):
    regression_test_df_schema(union_dependent_transformations_df, "union_dependent_transformations")
