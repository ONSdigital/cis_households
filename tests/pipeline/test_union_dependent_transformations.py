import pytest

from cishouseholds.merge import union_multiple_tables
from cishouseholds.pipeline.high_level_transformations import fill_forwards_transformations
from cishouseholds.pipeline.pipeline_stages import union_dependent_cleaning
from cishouseholds.pipeline.pipeline_stages import union_dependent_derivations


@pytest.mark.integration
def test_union_dependent_transformations(
    responses_v0_survey_ETL_output,
    responses_v1_survey_ETL_output,
    responses_v2_survey_ETL_output,
    responses_digital_ETL_output,
):
    """
    Check that pyspark can build a valid plan for union-dependent processing steps, given outputs from input processing.
    Should highlight any column name reference errors.
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
