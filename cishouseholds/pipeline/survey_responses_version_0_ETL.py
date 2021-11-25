from pyspark.sql import DataFrame

from cishouseholds.extract import get_files_to_be_processed
from cishouseholds.pipeline.ETL_scripts import extract_validate_transform_input_data
from cishouseholds.pipeline.input_variable_names import survey_responses_v0_variable_name_map
from cishouseholds.pipeline.load import update_table_and_log_source_files
from cishouseholds.pipeline.pipeline_stages import register_pipeline_stage
from cishouseholds.pipeline.re_cast_columns_from_string_map import survey_response_cast_to_double
from cishouseholds.pipeline.survey_responses_version_2_ETL import transform_survey_responses_generic
from cishouseholds.pipeline.timestamp_map import survey_responses_datetime_map
from cishouseholds.pipeline.validation_schema import survey_responses_v0_validation_schema


@register_pipeline_stage("survey_responses_version_0_ETL")
def survey_responses_version_0_ETL(**kwargs):
    """
    End to end processing of a IQVIA survey responses CSV file.
    """
    file_path_list = get_files_to_be_processed(**kwargs)
    if file_path_list:
        df = extract_validate_transform_input_data(
            file_path_list,
            survey_responses_v0_variable_name_map,
            survey_responses_datetime_map,
            survey_responses_v0_validation_schema,
            [transform_survey_responses_generic, transform_survey_responses_version_0_delta],
            "|",
            survey_response_cast_to_double,
        )
        update_table_and_log_source_files(
            df,
            "transformed_survey_responses_v0_data",
            "survey_response_source_file",
            "overwrite",
        )


def transform_survey_responses_version_0_delta(df: DataFrame) -> DataFrame:
    """
    Call functions to process input for iqvia version 0 survey deltas.
    """
    return df
