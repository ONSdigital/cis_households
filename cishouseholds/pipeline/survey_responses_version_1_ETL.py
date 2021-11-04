from pyspark.sql import DataFrame

from cishouseholds.derive import assign_filename_column
from cishouseholds.extract import get_files_by_date
from cishouseholds.pipeline.ETL_scripts import extract_validate_transform_input_data
from cishouseholds.pipeline.input_variable_names import survey_responses_v1_variable_name_map
from cishouseholds.pipeline.load import update_table_and_log_source_files
from cishouseholds.pipeline.pipeline_stages import register_pipeline_stage
from cishouseholds.pipeline.timestamp_map import survey_responses_datetime_map
from cishouseholds.pipeline.validation_schema import survey_responses_v1_validation_schema


@register_pipeline_stage("survey_responses_version_1_ETL")
def survey_responses_version_1_ETL(
    resource_path: str, latest_only: bool = False, start_date: str = None, end_date: str = None
):
    """
    End to end processing of a IQVIA survey responses CSV file.
    """
    file_path = get_files_by_date(resource_path, latest_only=latest_only, start_date=start_date, end_date=end_date)
    df = extract_validate_transform_input_data(
        file_path,
        survey_responses_v1_variable_name_map,
        survey_responses_datetime_map,
        survey_responses_v1_validation_schema,
        transform_survey_responses_version_1_delta,
        "|",
    )
    update_table_and_log_source_files(df, "transformed_survey_responses_v1_data", "survey_responses_v1_source_file")
    return df


def transform_survey_responses_version_1_delta(df: DataFrame) -> DataFrame:
    """
    Call functions to process input for iqvia version 1 survey deltas.
    """
    df = assign_filename_column(df, "survey_responses_v1_source_file")
    return df
