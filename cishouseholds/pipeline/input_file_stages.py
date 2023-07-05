from cishouseholds.pipeline.mapping import column_name_maps
from cishouseholds.pipeline.mapping import test_participant_data_cast_to_double
from cishouseholds.pipeline.mapping import test_survey_response_data_cast_to_double
from cishouseholds.pipeline.pipeline_stages import generate_input_processing_function
from cishouseholds.pipeline.timestamp_map import test_blood_sample_results_datetime_map
from cishouseholds.pipeline.timestamp_map import test_participant_data_datetime_map
from cishouseholds.pipeline.timestamp_map import test_survey_response_data_version_1_datetime_map
from cishouseholds.pipeline.timestamp_map import test_survey_response_data_version_2_datetime_map
from cishouseholds.pipeline.timestamp_map import test_swab_sample_results_datetime_map
from cishouseholds.pipeline.translate import translate_welsh_survey_responses
from cishouseholds.pipeline.validation_schema import validation_schemas
from cishouseholds.pipeline.version_specific_processing.multi_version_preprocessing import assign_has_been_columns
from cishouseholds.pipeline.version_specific_processing.multi_version_preprocessing import (
    derive_additional_v1_2_columns,
)
from cishouseholds.pipeline.version_specific_processing.test_participant_data_transformations import (
    transform_participant_extract_digital,
)
from cishouseholds.pipeline.version_specific_processing.test_survey_response_data_version_1_transformations import (
    clean_survey_responses_version_phm,
)
from cishouseholds.pipeline.version_specific_processing.test_survey_response_data_version_1_transformations import (
    phm_transformations,
)
from cishouseholds.pipeline.version_specific_processing.test_survey_response_data_version_2_transformations import (
    clean_survey_responses_version_2,
)
from cishouseholds.pipeline.version_specific_processing.test_survey_response_data_version_2_transformations import (
    transform_survey_responses_version_2_delta,
)


test_participant_data_parameters = {
    "stage_name": "test_participant_data_ETL",
    "dataset_name": "test_participant_data",
    "id_column": "participant_id",
    "validation_schema": validation_schemas["test_participant_data_schema"],
    "column_name_map": column_name_maps["test_participant_data_name_map"],
    "datetime_column_map": test_participant_data_datetime_map,
    "transformation_functions": [
        transform_participant_extract_digital,
    ],
    "sep": "|",
    "cast_to_double_list": test_participant_data_cast_to_double,
    "source_file_column": "participant_data_source_file",
}

test_survey_response_data_version_1_parameters = {
    "stage_name": "test_survey_response_data_version_1_ETL",
    "dataset_name": "test_survey_response_data_version_1",
    "id_column": "participant_completion_window_id",
    "validation_schema": validation_schemas["test_survey_response_data_version_1_schema"],
    "column_name_map": column_name_maps["test_survey_response_data_version_1_name_map"],
    "datetime_column_map": test_survey_response_data_version_1_datetime_map,
    "transformation_functions": [
        clean_survey_responses_version_phm,
        translate_welsh_survey_responses,
        phm_transformations,
    ],
    "sep": "|",
    "cast_to_double_list": test_survey_response_data_cast_to_double,
    "source_file_column": "survey_response_source_file",
    "survey_table": True,
}

test_survey_response_data_version_2_parameters = {
    "stage_name": "test_survey_response_data_version_2_ETL",
    "dataset_name": "test_survey_response_data_version_2",
    "id_column": "participant_completion_window_id",
    "validation_schema": validation_schemas["test_survey_response_data_version_2_schema"],
    "column_name_map": column_name_maps["test_survey_response_data_version_2_name_map"],
    "datetime_column_map": test_survey_response_data_version_2_datetime_map,
    "transformation_functions": [
        clean_survey_responses_version_2,
        derive_additional_v1_2_columns,
        transform_survey_responses_version_2_delta,
        assign_has_been_columns,
    ],
    "sep": "|",
    "cast_to_double_list": test_survey_response_data_cast_to_double,
    "source_file_column": "survey_response_source_file",
    "survey_table": True,
}

test_swab_sample_results_parameters = {
    "stage_name": "test_swab_sample_results_ETL",
    "dataset_name": "test_swab_sample_results",
    "id_column": "Sample",
    "validation_schema": validation_schemas["test_swab_sample_results_schema"],
    "datetime_column_map": test_swab_sample_results_datetime_map,
    "column_name_map": column_name_maps["test_swab_sample_results_name_map"],
    "transformation_functions": [],
    "sep": ",",
    "cast_to_double_list": [],
    "source_file_column": "swab_results_source_file",
    "write_mode": "append",
}

test_blood_sample_results_parameters = {
    "stage_name": "test_blood_sample_results_ETL",
    "dataset_name": "test_blood_sample_results",
    "id_column": "antibody_test_well_id",
    "validation_schema": validation_schemas["test_blood_sample_results_schema"],
    "datetime_column_map": test_blood_sample_results_datetime_map,
    "column_name_map": column_name_maps["test_blood_sample_results_name_map"],
    "transformation_functions": [],
    "sep": "|",
    "cast_to_double_list": ["Monoclonal undiluted quantitation (Colourimetric)"],
    "source_file_column": "blood_results_source_file",
    "write_mode": "append",
}

for parameters in [
    test_participant_data_parameters,
    test_survey_response_data_version_1_parameters,
    test_survey_response_data_version_2_parameters,
    test_swab_sample_results_parameters,
    test_blood_sample_results_parameters,
]:
    generate_input_processing_function(**parameters)  # type:ignore
