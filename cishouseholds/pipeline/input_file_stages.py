from cishouseholds.pipeline.high_level_transformations import assign_has_been_columns
from cishouseholds.pipeline.high_level_transformations import clean_survey_responses_version_1
from cishouseholds.pipeline.high_level_transformations import clean_survey_responses_version_2
from cishouseholds.pipeline.high_level_transformations import derive_additional_v1_2_columns
from cishouseholds.pipeline.high_level_transformations import pre_generic_digital_transformations
from cishouseholds.pipeline.high_level_transformations import transform_survey_responses_generic
from cishouseholds.pipeline.high_level_transformations import transform_survey_responses_version_0_delta
from cishouseholds.pipeline.high_level_transformations import transform_survey_responses_version_1_delta
from cishouseholds.pipeline.high_level_transformations import transform_survey_responses_version_2_delta
from cishouseholds.pipeline.high_level_transformations import transform_survey_responses_version_digital_delta
from cishouseholds.pipeline.mapping import column_name_maps
from cishouseholds.pipeline.mapping import survey_response_cast_to_double
from cishouseholds.pipeline.mapping import survey_response_cisd_cast_to_double
from cishouseholds.pipeline.pipeline_stages import generate_input_processing_function
from cishouseholds.pipeline.timestamp_map import cis_digital_datetime_map
from cishouseholds.pipeline.timestamp_map import survey_responses_v0_datetime_map
from cishouseholds.pipeline.timestamp_map import survey_responses_v1_datetime_map
from cishouseholds.pipeline.timestamp_map import survey_responses_v2_datetime_map
from cishouseholds.pipeline.translate import translate_welsh_survey_responses_version_digital
from cishouseholds.pipeline.validation_schema import validation_schemas

survey_responses_v2_parameters = {
    "stage_name": "survey_responses_version_2_ETL",
    "dataset_name": "survey_responses_v2",
    "id_column": "visit_id",
    "validation_schema": validation_schemas["survey_responses_v2_validation_schema"],
    "column_name_map": column_name_maps["survey_responses_v2_variable_name_map"],
    "datetime_column_map": survey_responses_v2_datetime_map,
    "transformation_functions": [
        transform_survey_responses_generic,
        clean_survey_responses_version_2,
        derive_additional_v1_2_columns,
        transform_survey_responses_version_2_delta,
        assign_has_been_columns,
    ],
    "sep": "|",
    "cast_to_double_list": survey_response_cast_to_double,
    "source_file_column": "survey_response_source_file",
}

survey_responses_v1_parameters = {
    "stage_name": "survey_responses_version_1_ETL",
    "dataset_name": "survey_responses_v1",
    "id_column": "visit_id",
    "validation_schema": validation_schemas["survey_responses_v1_validation_schema"],
    "column_name_map": column_name_maps["survey_responses_v1_variable_name_map"],
    "datetime_column_map": survey_responses_v1_datetime_map,
    "transformation_functions": [
        transform_survey_responses_generic,
        clean_survey_responses_version_1,
        derive_additional_v1_2_columns,
        transform_survey_responses_version_1_delta,
    ],
    "sep": "|",
    "cast_to_double_list": survey_response_cast_to_double,
    "source_file_column": "survey_response_source_file",
}

survey_responses_v0_parameters = {
    "stage_name": "survey_responses_version_0_ETL",
    "dataset_name": "survey_responses_v0",
    "id_column": "visit_id",
    "validation_schema": validation_schemas["survey_responses_v0_validation_schema"],
    "column_name_map": column_name_maps["survey_responses_v0_variable_name_map"],
    "datetime_column_map": survey_responses_v0_datetime_map,
    "transformation_functions": [
        transform_survey_responses_generic,
        transform_survey_responses_version_0_delta,
    ],
    "sep": "|",
    "cast_to_double_list": survey_response_cast_to_double,
    "source_file_column": "survey_response_source_file",
}

cis_digital_parameters = {
    "stage_name": "survey_responses_version_digital_ETL",
    "dataset_name": "survey_responses_digital",
    "id_column": "participant_completion_window_id",
    "validation_schema": validation_schemas["cis_digital_validation_schema"],
    "datetime_column_map": cis_digital_datetime_map,
    "transformation_functions": [
        translate_welsh_survey_responses_version_digital,
        pre_generic_digital_transformations,
        transform_survey_responses_generic,
        transform_survey_responses_version_digital_delta,
        assign_has_been_columns,
    ],
    "sep": "|",
    "cast_to_double_list": survey_response_cisd_cast_to_double,
    "source_file_column": "survey_response_source_file",
}

for parameters in [
    cis_digital_parameters,
    survey_responses_v2_parameters,
    survey_responses_v1_parameters,
    survey_responses_v0_parameters,
]:
    generate_input_processing_function(**parameters)  # type:ignore
