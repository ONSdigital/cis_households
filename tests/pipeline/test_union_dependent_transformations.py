import pytest

from cishouseholds.merge import union_multiple_tables
from cishouseholds.pipeline.job_transformations import job_transformations
from cishouseholds.pipeline.post_union_transformations import post_union_transformations
from cishouseholds.pipeline.symptom_transformations import symptom_transformations


@pytest.fixture
def unioned_tables(
    responses_v0_survey_ETL_output,
    responses_v1_survey_ETL_output,
    responses_v2_survey_ETL_output,
    responses_digital_ETL_output,
):
    return union_multiple_tables(
        [
            responses_v0_survey_ETL_output,
            responses_v1_survey_ETL_output,
            responses_v2_survey_ETL_output,
            responses_digital_ETL_output,
        ]
    )


@pytest.mark.parametrize("function", [post_union_transformations, job_transformations, symptom_transformations])
def test_union_dependent_transformations(unioned_tables, function):
    """
    Check that pyspark can build a valid plan for union-dependent processing steps, given outputs from input processing.
    Should highlight any column name reference errors.
    """
    df = function(unioned_tables)
