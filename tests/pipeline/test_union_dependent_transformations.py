import pytest

from cishouseholds.merge import union_multiple_tables
from cishouseholds.pipeline.high_level_transformations import fill_forwards_transformations
from cishouseholds.pipeline.pipeline_stages import union_dependent_cleaning
from cishouseholds.pipeline.pipeline_stages import union_dependent_derivations
from cishouseholds.pyspark_utils import get_or_create_spark_session


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
    df_2 = get_or_create_spark_session().createDataFrame(
        df.rdd, schema=df.schema
    )  # breaks lineage to avoid Java OOM Error
    df_3 = union_dependent_cleaning(df_2)
    df_4 = get_or_create_spark_session().createDataFrame(
        df_3.rdd, schema=df_3.schema
    )  # breaks lineage to avoid Java OOM Error
    df_5 = union_dependent_derivations(df_4)
