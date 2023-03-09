from cishouseholds.pipeline.mapping import column_name_maps
from cishouseholds.pipeline.mapping import survey_response_cast_to_double
from cishouseholds.pipeline.mapping import survey_response_cisd_cast_to_double
from cishouseholds.pipeline.pipeline_stages import generate_input_processing_function
from cishouseholds.pipeline.timestamp_map import blood_datetime_map
from cishouseholds.pipeline.timestamp_map import cis_digital_datetime_map
from cishouseholds.pipeline.timestamp_map import historical_blood_datetime_map
from cishouseholds.pipeline.timestamp_map import lab_results_glasgow_datetime_map
from cishouseholds.pipeline.timestamp_map import phm_datetime_map
from cishouseholds.pipeline.timestamp_map import survey_responses_v0_datetime_map
from cishouseholds.pipeline.timestamp_map import survey_responses_v1_datetime_map
from cishouseholds.pipeline.timestamp_map import survey_responses_v2_datetime_map
from cishouseholds.pipeline.validation_schema import validation_schemas
from cishouseholds.pipeline.version_specific_processing.digital_transformations import digital_responses_preprocessing
from cishouseholds.pipeline.version_specific_processing.digital_transformations import (
    transform_survey_responses_version_digital_delta,
)
from cishouseholds.pipeline.version_specific_processing.mult_version import assign_has_been_columns
from cishouseholds.pipeline.version_specific_processing.mult_version import derive_additional_v1_2_columns
from cishouseholds.pipeline.version_specific_processing.participant_extract import transform_participant_extract_digital
from cishouseholds.pipeline.version_specific_processing.participant_extract import (
    translate_welsh_survey_responses_version_digital,
)
from cishouseholds.pipeline.version_specific_processing.phm_transformations import (
    transform_survey_responses_version_phm_delta,
)
from cishouseholds.pipeline.version_specific_processing.v0_transformations import (
    transform_survey_responses_version_0_delta,
)
from cishouseholds.pipeline.version_specific_processing.v1_transformations import clean_survey_responses_version_1
from cishouseholds.pipeline.version_specific_processing.v1_transformations import (
    transform_survey_responses_version_1_delta,
)
from cishouseholds.pipeline.version_specific_processing.v2_transformations import clean_survey_responses_version_2
from cishouseholds.pipeline.version_specific_processing.v2_transformations import (
    transform_survey_responses_version_2_delta,
)

participant_extract_digital_parameters = {
    "stage_name": "participant_extract_digital_ETL",
    "dataset_name": "participant_extract_digital",
    "id_column": "participant_id",
    "validation_schema": validation_schemas["participant_extract_digital_validation_schema"],
    "column_name_map": column_name_maps["participant_extract_digital_name_map"],
    "datetime_column_map": cis_digital_datetime_map,
    "transformation_functions": [
        transform_participant_extract_digital,
    ],
    "sep": "|",
    "cast_to_double_list": survey_response_cisd_cast_to_double,
    "source_file_column": "participant_extract_source_file",
}

phm_participant_parameters = {
    "stage_name": "phm_participant_extract_ETL",
    "dataset_name": "phm_participant_extract",
    "id_column": "participant_id",
    "validation_schema": validation_schemas["phm_participant_extract_validation_schema"],
    "datetime_column_map": phm_datetime_map,
    "transformation_functions": [],
    "sep": "|",
    "cast_to_double_list": [],
    "source_file_column": "phm_participant_extract_gsource_file",
}

phm_parameters = {
    "stage_name": "survey_responses_version_phm_ETL",
    "dataset_name": "survey_responses_phm",
    "id_column": "participant_completion_window_id",
    "validation_schema": validation_schemas["phm_survey_validation_schema"],
    "datetime_column_map": phm_datetime_map,
    "transformation_functions": [
        transform_survey_responses_version_phm_delta,
    ],
    "sep": "|",
    "cast_to_double_list": [],
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
        digital_responses_preprocessing,
        transform_survey_responses_version_digital_delta,
        assign_has_been_columns,
    ],
    "sep": "|",
    "cast_to_double_list": survey_response_cisd_cast_to_double,
    "source_file_column": "survey_response_source_file",
}

survey_responses_v2_parameters = {
    "stage_name": "survey_responses_version_2_ETL",
    "dataset_name": "survey_responses_v2",
    "id_column": "visit_id",
    "validation_schema": validation_schemas["survey_responses_v2_validation_schema"],
    "column_name_map": column_name_maps["survey_responses_v2_variable_name_map"],
    "datetime_column_map": survey_responses_v2_datetime_map,
    "transformation_functions": [
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
        transform_survey_responses_version_0_delta,
    ],
    "sep": "|",
    "cast_to_double_list": survey_response_cast_to_double,
    "source_file_column": "survey_response_source_file",
}

brants_bridge_parameters = {
    "stage_name": "brants_bridge_ETL",
    "dataset_name": "brants_bridge",
    "id_column": "swab_sample_barcode",
    "validation_schema": validation_schemas["brants_bridge_schema"],
    "datetime_column_map": None,
    "column_name_map": column_name_maps["brants_bridge_variable_name_map"],
    "transformation_functions": [],
    "sep": ",",
    "cast_to_double_list": [],
    "source_file_column": "brants_bridge_source_file",
    "write_mode": "append",
}
swab_results_parameters = {
    "stage_name": "swab_results_ETL",
    "dataset_name": "swab_results",
    "id_column": "Sample",
    "validation_schema": validation_schemas["lab_results_glasgow_schema"],
    "datetime_column_map": lab_results_glasgow_datetime_map,
    "column_name_map": column_name_maps["lab_results_glasgow_variable_name_map"],
    "transformation_functions": [],
    "sep": ",",
    "cast_to_double_list": [],
    "source_file_column": "swab_results_source_file",
    "write_mode": "append",
}

blood_results_parameters = {
    "stage_name": "blood_results_ETL",
    "dataset_name": "blood_results",
    "id_column": "antibody_test_well_id",
    "validation_schema": validation_schemas["blood_validation_schema"],
    "datetime_column_map": blood_datetime_map,
    "column_name_map": column_name_maps["blood_variable_name_map"],
    "transformation_functions": [],
    "sep": "|",
    "cast_to_double_list": ["Monoclonal undiluted quantitation (Colourimetric)"],
    "source_file_column": "blood_results_source_file",
    "write_mode": "append",
}

historical_blood_results_parameters = {
    "stage_name": "historical_blood_results",
    "dataset_name": "historical_blood_results",
    "id_column": "antibody_test_well_id",
    "validation_schema": validation_schemas["historical_blood_validation_schema"],
    "datetime_column_map": historical_blood_datetime_map,
    "transformation_functions": [],
    "sep": "|",
    "cast_to_double_list": [],
    "source_file_column": "historical_blood_results_source_file",
    "write_mode": "append",
}

for parameters in [
    participant_extract_digital_parameters,
    cis_digital_parameters,
    survey_responses_v2_parameters,
    survey_responses_v1_parameters,
    survey_responses_v0_parameters,
    swab_results_parameters,
    blood_results_parameters,
    historical_blood_results_parameters,
    brants_bridge_parameters,
]:
    generate_input_processing_function(**parameters)  # type:ignore
