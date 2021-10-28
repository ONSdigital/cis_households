from pyspark.sql import DataFrame

from cishouseholds.derive import assign_filename_column
from cishouseholds.pipeline.ETL_scripts import extract_validate_transform_input_data
from cishouseholds.pipeline.input_variable_names import survey_responses_v0_variable_name_map
from cishouseholds.pipeline.load import update_table_and_log_source_files
from cishouseholds.pipeline.pipeline_stages import register_pipeline_stage
from cishouseholds.pipeline.timestamp_map import survey_responses_datetime_map
from cishouseholds.pipeline.validation_schema import survey_responses_v0_validation_schema


@register_pipeline_stage("survey_responses_version_0_ETL")
def survey_responses_version_0_ETL(resource_path: str):
    """
    End to end processing of a IQVIA survey responses CSV file.
    """
    df = extract_validate_transform_input_data(
        resource_path,
        survey_responses_v0_variable_name_map,
        survey_responses_datetime_map,
        survey_responses_v0_validation_schema,
        transform_survey_responses_version_0_delta,
        "|",
    )
    update_table_and_log_source_files(df, "transformed_survey_responses_v0_data", "survey_responses_v0_source_file")
    return df


def transform_survey_responses_version_0_delta(df: DataFrame) -> DataFrame:
    """
    Call functions to process input for iqvia version 0 survey deltas.
    """
    df = assign_filename_column(df, "survey_responses_v0_source_file")
    return df
