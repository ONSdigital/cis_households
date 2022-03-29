from typing import Callable
from typing import List

from pyspark.sql import functions as F

from cishouseholds.derive import assign_filename_column
from cishouseholds.edit import cast_columns_from_string
from cishouseholds.edit import convert_columns_to_timestamps
from cishouseholds.edit import rename_column_names
from cishouseholds.edit import update_from_lookup_df
from cishouseholds.extract import extract_lookup_csv
from cishouseholds.pipeline.blood_delta_ETL import add_historical_fields
from cishouseholds.pipeline.blood_delta_ETL import transform_blood_delta
from cishouseholds.pipeline.cast_columns_from_string_map import survey_response_cast_to_double
from cishouseholds.pipeline.config import get_config
from cishouseholds.pipeline.config import get_secondary_config
from cishouseholds.pipeline.historical_blood_ETL import add_fields
from cishouseholds.pipeline.input_variable_names import blood_variable_name_map
from cishouseholds.pipeline.input_variable_names import historical_blood_variable_name_map
from cishouseholds.pipeline.input_variable_names import survey_responses_v0_variable_name_map
from cishouseholds.pipeline.input_variable_names import survey_responses_v1_variable_name_map
from cishouseholds.pipeline.input_variable_names import survey_responses_v2_variable_name_map
from cishouseholds.pipeline.input_variable_names import swab_variable_name_map
from cishouseholds.pipeline.input_variable_names import unassayed_bloods_variable_name_map
from cishouseholds.pipeline.load import get_full_table_name
from cishouseholds.pipeline.load import update_table
from cishouseholds.pipeline.pipeline_stages import generate_input_processing_function
from cishouseholds.pipeline.survey_responses_version_0_ETL import transform_survey_responses_version_0_delta
from cishouseholds.pipeline.survey_responses_version_1_ETL import transform_survey_responses_version_1_delta
from cishouseholds.pipeline.survey_responses_version_2_ETL import derive_additional_v1_2_columns
from cishouseholds.pipeline.survey_responses_version_2_ETL import transform_survey_responses_generic
from cishouseholds.pipeline.survey_responses_version_2_ETL import transform_survey_responses_version_2_delta
from cishouseholds.pipeline.swab_delta_ETL import transform_swab_delta
from cishouseholds.pipeline.swab_delta_ETL_testKit import transform_swab_delta_testKit
from cishouseholds.pipeline.timestamp_map import blood_datetime_map
from cishouseholds.pipeline.timestamp_map import survey_responses_v0_datetime_map
from cishouseholds.pipeline.timestamp_map import survey_responses_v1_datetime_map
from cishouseholds.pipeline.timestamp_map import survey_responses_v2_datetime_map
from cishouseholds.pipeline.timestamp_map import swab_datetime_map
from cishouseholds.pipeline.unassayed_blood_ETL import transform_unassayed_blood
from cishouseholds.pipeline.validation_schema import blood_validation_schema
from cishouseholds.pipeline.validation_schema import csv_lookup_schema
from cishouseholds.pipeline.validation_schema import historical_blood_validation_schema
from cishouseholds.pipeline.validation_schema import survey_responses_v0_validation_schema
from cishouseholds.pipeline.validation_schema import survey_responses_v1_validation_schema
from cishouseholds.pipeline.validation_schema import survey_responses_v2_validation_schema
from cishouseholds.pipeline.validation_schema import swab_validation_schema
from cishouseholds.pipeline.validation_schema import swab_validation_schema_testKit
from cishouseholds.pipeline.validation_schema import unassayed_blood_validation_schema
from cishouseholds.pyspark_utils import convert_cerberus_schema_to_pyspark
from cishouseholds.pyspark_utils import get_or_create_spark_session


blood_delta_parameters = {
    "stage_name": "blood_delta_ETL",
    "dataset_name": "blood_delta",
    "id_column": "blood_sample_barcode",
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
    "dataset_name": "swab_delta",
    "id_column": "swab_sample_barcode",
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
    "dataset_name": "swab_testkit_delta",
    "id_column": "swab_sample_barcode",
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
    "dataset_name": "survey_responses_v2",
    "id_column": "visit_id",
    "validation_schema": survey_responses_v2_validation_schema,
    "column_name_map": survey_responses_v2_variable_name_map,
    "datetime_column_map": survey_responses_v2_datetime_map,
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
    "dataset_name": "survey_responses_v1",
    "id_column": "visit_id",
    "validation_schema": survey_responses_v1_validation_schema,
    "column_name_map": survey_responses_v1_variable_name_map,
    "datetime_column_map": survey_responses_v1_datetime_map,
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
    "dataset_name": "survey_responses_v0",
    "id_column": "visit_id",
    "validation_schema": survey_responses_v0_validation_schema,
    "column_name_map": survey_responses_v0_variable_name_map,
    "datetime_column_map": survey_responses_v0_datetime_map,
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
    "dataset_name": "unassayed_blood_delta",
    "id_column": "blood_sample_barcode",
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
    "dataset_name": "historical_blood_delta",
    "id_column": "blood_sample_barcode",
    "validation_schema": historical_blood_validation_schema,
    "column_name_map": historical_blood_variable_name_map,
    "datetime_column_map": blood_datetime_map,
    "transformation_functions": [transform_blood_delta, add_fields],
    "output_table_name": "transformed_blood_test_data",
    "source_file_column": "blood_test_source_file",
}

for parameters in [
    blood_delta_parameters,
    swab_delta_parameters,
    swab_delta_parameters_testKit,
    survey_responses_v2_parameters,
    survey_responses_v1_parameters,
    survey_responses_v0_parameters,
    unassayed_blood_delta_parameters,
    historical_blood_parameters,
]:
    generate_input_processing_function(**parameters)


def extract_validate_transform_input_data(
    dataset_name: str,
    id_column: str,
    resource_path: list,
    variable_name_map: dict,
    datetime_map: dict,
    validation_schema: dict,
    transformation_functions: List[Callable],
    source_file_column: str,
    sep: str = ",",
    cast_to_double_columns_list: list = [],
    include_hadoop_read_write: bool = False,
):
    if include_hadoop_read_write:
        storage_config = get_config()["storage"]
        csv_location = storage_config["csv_editing_file"]
        filter_config = get_secondary_config(storage_config["filter_config_file"])

    df = extract_input_data(resource_path, validation_schema, sep)
    df = rename_column_names(df, variable_name_map)
    df = assign_filename_column(df, source_file_column)  # Must be called before update_from_csv_lookup

    filtered_df = None
    if include_hadoop_read_write and filter_config is not None:
        if dataset_name in filter_config:
            filter_ids = filter_config[dataset_name]
            filtered_df = df.filter(F.col(id_column).isin(filter_ids))
            update_table(df, f"raw_{dataset_name}")
            update_table(filtered_df, f"{dataset_name}_rows_extracted")

            df = df.filter(~F.col(id_column).isin(filter_ids))

        if csv_location is not None:
            lookup_df = extract_lookup_csv(csv_location, csv_lookup_schema)
            df = update_from_lookup_df(df, lookup_df, id_column=id_column, dataset_name=dataset_name)

    df = convert_columns_to_timestamps(df, datetime_map)
    df = cast_columns_from_string(df, cast_to_double_columns_list, "double")

    for transformation_function in transformation_functions:
        df = transformation_function(df)
    return df, filtered_df


def extract_input_data(file_paths: list, validation_schema: dict, sep: str):
    spark_session = get_or_create_spark_session()
    spark_schema = convert_cerberus_schema_to_pyspark(validation_schema) if validation_schema is not None else None
    return spark_session.read.csv(
        file_paths,
        header=True,
        schema=spark_schema,
        ignoreLeadingWhiteSpace=True,
        ignoreTrailingWhiteSpace=True,
        sep=sep,
    )


def extract_from_table(table_name: str):
    spark_session = get_or_create_spark_session()
    return spark_session.sql(f"SELECT * FROM {get_full_table_name(table_name)}")


def extract_df_list(files):
    dfs = {}
    for key, file in files.items():
        if file["file"] == "" or file["file"] is None:
            dfs[key] = None
        elif file["type"] == "table":
            dfs[key] = extract_from_table(file["file"])
        else:
            dfs[key] = extract_input_data(file_paths=file["file"], validation_schema=None, sep=",")

    return dfs
