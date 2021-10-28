from itertools import chain

from pyspark.accumulators import AddingAccumulatorParam
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

from cishouseholds.derive import assign_filename_column
from cishouseholds.edit import convert_columns_to_timestamps
from cishouseholds.edit import rename_column_names
from cishouseholds.edit import update_schema_names
from cishouseholds.edit import update_schema_types
from cishouseholds.extract import read_csv_to_pyspark_df
from cishouseholds.pipeline.input_variable_names import survey_responses_v1_variable_name_map
from cishouseholds.pipeline.load import update_table_and_log_source_files
from cishouseholds.pipeline.pipeline_stages import register_pipeline_stage
from cishouseholds.pipeline.timestamp_map import survey_responses_v2_datetime_map
from cishouseholds.pipeline.validation_schema import survey_responses_v1_validation_schema
from cishouseholds.pyspark_utils import convert_cerberus_schema_to_pyspark
from cishouseholds.pyspark_utils import get_or_create_spark_session
from cishouseholds.validate import validate_and_filter

# from cishouseholds.derive import assign_work_person_facing_now


@register_pipeline_stage("survey_responses_version_2_ETL")
def survey_responses_version_1_ETL(resource_path: str):
    """
    End to end processing of a IQVIA survey responses CSV file.
    """
    df = extract_validate_transform_survey_responses_version_1_delta(resource_path)
    update_table_and_log_source_files(df, "transformed_survey_responses_v2_data", "survey_responses_v2_source_file")
    return df


def extract_validate_transform_survey_responses_version_1_delta(resource_path: str):
    spark_session = get_or_create_spark_session()
    df = extract_survey_responses_version_1_delta(spark_session, resource_path)
    df = rename_column_names(df, survey_responses_v1_variable_name_map)
    df = convert_columns_to_timestamps(df, survey_responses_v2_datetime_map)
    _survey_responses_v2_validation_schema = update_schema_names(
        survey_responses_v1_validation_schema, survey_responses_v1_variable_name_map
    )
    survey_responses_v2_datetime_map_list = list(chain(*list(survey_responses_v2_datetime_map.values())))
    _survey_responses_v2_validation_schema = update_schema_types(
        _survey_responses_v2_validation_schema, survey_responses_v2_datetime_map_list, {"type": "timestamp"}
    )

    error_accumulator = spark_session.sparkContext.accumulator(
        value=[], accum_param=AddingAccumulatorParam(zero_value=[])
    )

    df = validate_and_filter(df, _survey_responses_v2_validation_schema, error_accumulator)
    df = transform_survey_responses_version_1_delta(df)
    return df


def extract_survey_responses_version_1_delta(spark_session: SparkSession, resource_path: str):
    iqvia_v1_spark_schema = convert_cerberus_schema_to_pyspark(survey_responses_v1_validation_schema)
    raw_iqvia_v1_data_header = "|".join(survey_responses_v1_validation_schema.keys())
    df = read_csv_to_pyspark_df(spark_session, resource_path, raw_iqvia_v1_data_header, iqvia_v1_spark_schema, sep="|")
    return df


def transform_survey_responses_version_1_delta(df: DataFrame) -> DataFrame:
    """
    Call functions to process input for iqvia version 2 survey deltas.
    """
    df = assign_filename_column(df, "survey_responses_v2_source_file")
    return df
