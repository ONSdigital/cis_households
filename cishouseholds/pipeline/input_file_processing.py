from cishouseholds.extract import get_files_to_be_processed
from cishouseholds.pipeline.blood_delta_ETL import add_historical_fields
from cishouseholds.pipeline.blood_delta_ETL import transform_blood_delta
from cishouseholds.pipeline.cast_columns_from_string_map import survey_response_cast_to_double
from cishouseholds.pipeline.ETL_scripts import extract_validate_transform_input_data
from cishouseholds.pipeline.historical_blood_ETL import add_fields
from cishouseholds.pipeline.input_variable_names import blood_variable_name_map
from cishouseholds.pipeline.input_variable_names import historical_blood_variable_name_map
from cishouseholds.pipeline.input_variable_names import survey_responses_v0_variable_name_map
from cishouseholds.pipeline.input_variable_names import survey_responses_v1_variable_name_map
from cishouseholds.pipeline.input_variable_names import survey_responses_v2_variable_name_map
from cishouseholds.pipeline.input_variable_names import swab_variable_name_map
from cishouseholds.pipeline.input_variable_names import unassayed_bloods_variable_name_map
from cishouseholds.pipeline.load import update_table_and_log_source_files
from cishouseholds.pipeline.pipeline_stages import register_pipeline_stage
from cishouseholds.pipeline.survey_responses_version_0_ETL import transform_survey_responses_version_0_delta
from cishouseholds.pipeline.survey_responses_version_1_ETL import transform_survey_responses_version_1_delta
from cishouseholds.pipeline.survey_responses_version_2_ETL import derive_additional_v1_2_columns
from cishouseholds.pipeline.survey_responses_version_2_ETL import transform_survey_responses_generic
from cishouseholds.pipeline.survey_responses_version_2_ETL import transform_survey_responses_version_2_delta
from cishouseholds.pipeline.swab_delta_ETL import transform_swab_delta
from cishouseholds.pipeline.swab_delta_ETL_testKit import transform_swab_delta_testKit
from cishouseholds.pipeline.timestamp_map import blood_datetime_map
from cishouseholds.pipeline.timestamp_map import survey_responses_datetime_map
from cishouseholds.pipeline.timestamp_map import swab_datetime_map
from cishouseholds.pipeline.unassayed_blood_ETL import transform_unassayed_blood
from cishouseholds.pipeline.validation_schema import blood_validation_schema
from cishouseholds.pipeline.validation_schema import historical_blood_validation_schema
from cishouseholds.pipeline.validation_schema import survey_responses_v0_validation_schema
from cishouseholds.pipeline.validation_schema import survey_responses_v1_validation_schema
from cishouseholds.pipeline.validation_schema import survey_responses_v2_validation_schema
from cishouseholds.pipeline.validation_schema import swab_validation_schema
from cishouseholds.pipeline.validation_schema import swab_validation_schema_testKit
from cishouseholds.pipeline.validation_schema import unassayed_blood_validation_schema
from cishouseholds.validate import validate_files


blood_delta_parameters = {
    "stage_name": "blood_delta_ETL",
    "validation_schema": blood_validation_schema,
    "column_name_map": blood_variable_name_map,
    "datetime_column_map": blood_datetime_map,
    "transformation_functions": [transform_blood_delta, add_historical_fields],
    "output_table_name": "transformed_blood_test_data",
    "source_file_column": "blood_test_source_file",
    "write_mode": "append",
}

swab_delta_parameters = {
    "stage_name": "swab_delta_ETL",
    "validation_schema": swab_validation_schema,
    "column_name_map": swab_variable_name_map,
    "datetime_column_map": swab_datetime_map,
    "transformation_functions": [transform_swab_delta],
    "output_table_name": "transformed_swab_test_data",
    "source_file_column": "swab_test_source_file",
    "write_mode": "append",
}

swab_delta_parameters_testKit = {
    "stage_name": "swab_testKit_delta_ETL",
    "validation_schema": swab_validation_schema_testKit,
    "column_name_map": swab_variable_name_map,
    "datetime_column_map": swab_datetime_map,
    "transformation_functions": [transform_swab_delta_testKit, transform_swab_delta],
    "output_table_name": "transformed_swab_test_data",
    "source_file_column": "swab_test_source_file",
    "write_mode": "append",
}

survey_responses_v2_parameters = {
    "stage_name": "survey_responses_version_2_ETL",
    "validation_schema": survey_responses_v2_validation_schema,
    "column_name_map": survey_responses_v2_variable_name_map,
    "datetime_column_map": survey_responses_datetime_map,
    "transformation_functions": [
        transform_survey_responses_generic,
        derive_additional_v1_2_columns,
        transform_survey_responses_version_2_delta,
    ],
    "sep": "|",
    "cast_to_double_list": survey_response_cast_to_double,
    "output_table_name": "transformed_survey_responses_v2_data",
    "source_file_column": "survey_response_source_file",
}

survey_responses_v1_parameters = {
    "stage_name": "survey_responses_version_1_ETL",
    "validation_schema": survey_responses_v1_validation_schema,
    "column_name_map": survey_responses_v1_variable_name_map,
    "datetime_column_map": survey_responses_datetime_map,
    "transformation_functions": [
        transform_survey_responses_generic,
        derive_additional_v1_2_columns,
        transform_survey_responses_version_1_delta,
    ],
    "sep": "|",
    "cast_to_double_list": survey_response_cast_to_double,
    "output_table_name": "transformed_survey_responses_v1_data",
    "source_file_column": "survey_response_source_file",
}

survey_responses_v0_parameters = {
    "stage_name": "survey_responses_version_0_ETL",
    "validation_schema": survey_responses_v0_validation_schema,
    "column_name_map": survey_responses_v0_variable_name_map,
    "datetime_column_map": survey_responses_datetime_map,
    "transformation_functions": [
        transform_survey_responses_generic,
        transform_survey_responses_version_0_delta,
    ],
    "sep": "|",
    "cast_to_double_list": survey_response_cast_to_double,
    "output_table_name": "transformed_survey_responses_v0_data",
    "source_file_column": "survey_response_source_file",
}

unassayed_blood_delta_parameters = {
    "stage_name": "unassayed_blood_ETL",
    "validation_schema": unassayed_blood_validation_schema,
    "column_name_map": unassayed_bloods_variable_name_map,
    "datetime_column_map": blood_datetime_map,
    "transformation_functions": [transform_unassayed_blood],
    "output_table_name": "unassayed_blood_sample_data",
    "source_file_column": "unassayed_blood_source_file",
    "write_mode": "append",
}

historical_blood_parameters = {
    "stage_name": "historical_blood_ETL",
    "validation_schema": historical_blood_validation_schema,
    "column_name_map": historical_blood_variable_name_map,
    "datetime_column_map": blood_datetime_map,
    "transformation_functions": [transform_blood_delta, add_fields],
    "output_table_name": "transformed_blood_test_data",
    "source_file_column": "blood_test_source_file",
}


def generate_input_processing_function(
    stage_name,
    validation_schema,
    column_name_map,
    datetime_column_map,
    transformation_functions,
    output_table_name,
    source_file_column,
    write_mode="overwrite",
    sep=",",
    cast_to_double_list=[],
    include_hadoop_read_write=True,
):
    """
    Generate an input file processing stage function and register it.

    Returns dataframe for use in testing.

    Parameters
    ----------
    include_hadoop_read_write
        set to False for use in testing on non-hadoop environments

    Notes
    -----
    See underlying functions for other parameter documentation.
    """

    @register_pipeline_stage(stage_name)
    def _inner_function(**kwargs):
        file_path_list = [kwargs["resource_path"]]
        if include_hadoop_read_write:
            file_path_list = get_files_to_be_processed(**kwargs)
        if not file_path_list:
            print(f"        - No files selected in {kwargs['resource_path']}")  # functional
            return

        valid_file_paths = validate_files(file_path_list, validation_schema, sep=sep)
        if not valid_file_paths:
            print(f"        - No valid files found in: {kwargs['resource_path']}.")  # functional
            return

        df = extract_validate_transform_input_data(
            file_path_list,
            column_name_map,
            datetime_column_map,
            validation_schema,
            transformation_functions,
            sep,
            cast_to_double_list,
        )
        if include_hadoop_read_write:
            update_table_and_log_source_files(df, output_table_name, source_file_column, write_mode)
        return df

    _inner_function.__name__ = stage_name
    return _inner_function


for parameters in [
    blood_delta_parameters,
    swab_delta_parameters,
    survey_responses_v2_parameters,
    survey_responses_v1_parameters,
    survey_responses_v0_parameters,
    unassayed_blood_delta_parameters,
    historical_blood_parameters,
]:
    generate_input_processing_function(**parameters)
