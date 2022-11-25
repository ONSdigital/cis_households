import pytest

from cishouseholds.merge import union_multiple_tables
from cishouseholds.pipeline.high_level_transformations import add_pattern_matching_flags
from cishouseholds.pipeline.high_level_transformations import fill_forwards_transformations
from cishouseholds.pipeline.pipeline_stages import union_dependent_cleaning
from cishouseholds.pipeline.pipeline_stages import union_dependent_derivations


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


@pytest.mark.integration
@pytest.mark.parametrize(
    "function", [fill_forwards_transformations, union_dependent_cleaning, union_dependent_derivations]
)
def test_union_dependent_transformations(unioned_tables, function):
    """
    Check that pyspark can build a valid plan for union-dependent processing steps, given outputs from input processing.
    Should highlight any column name reference errors.
    """
    df = function(unioned_tables)
