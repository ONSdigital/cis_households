from itertools import chain

from pyspark.accumulators import AddingAccumulatorParam
from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession

from cishouseholds.derive import assign_column_uniform_value
from cishouseholds.derive import assign_filename_column
from cishouseholds.derive import assign_test_target
from cishouseholds.edit import convert_columns_to_timestamps
from cishouseholds.edit import rename_column_names
from cishouseholds.edit import update_schema_names
from cishouseholds.edit import update_schema_types
from cishouseholds.extract import read_csv_to_pyspark_df
from cishouseholds.pipeline.input_variable_names import unprocessed_bloods_variable_name_map
from cishouseholds.pipeline.load import update_table_and_log_source_files
from cishouseholds.pipeline.pipeline_stages import register_pipeline_stage
from cishouseholds.pipeline.timestamp_map import blood_datetime_map
from cishouseholds.pipeline.validation_schema import unprocessed_blood_validation_schema
from cishouseholds.pyspark_utils import convert_cerberus_schema_to_pyspark
from cishouseholds.pyspark_utils import get_or_create_spark_session
from cishouseholds.validate import validate_and_filter

# from cishouseholds.compare import prepare_for_union


@register_pipeline_stage("unprocessed_blood_ETL")
def unprocessed_blood_ETL(resource_path: str):
    df = extract_validate_transform_unprocessed_blood(resource_path)
    update_table_and_log_source_files(df, "unprocessed_blood_test_data", "unprocessed_blood_source_file")
    return df


def extract_validate_transform_unprocessed_blood(resource_path: str):
    spark_session = get_or_create_spark_session()

    df = extract_unprocessed_blood(spark_session, resource_path)
    df = rename_column_names(df, unprocessed_bloods_variable_name_map)
    df = convert_columns_to_timestamps(df, blood_datetime_map)

    _unprocessed_blood_validation_schema = update_schema_names(
        unprocessed_blood_validation_schema, unprocessed_bloods_variable_name_map
    )

    df = convert_columns_to_timestamps(df, blood_datetime_map)
    blood_datetime_map_list = list(chain(*list(blood_datetime_map.values())))
    _unprocessed_blood_validation_schema = update_schema_types(
        _unprocessed_blood_validation_schema, blood_datetime_map_list, {"type": "timestamp"}
    )

    error_accumulator = spark_session.sparkContext.accumulator(
        value=[], accum_param=AddingAccumulatorParam(zero_value=[])
    )
    df = validate_and_filter(df, _unprocessed_blood_validation_schema, error_accumulator)
    df = transform_unprocessed_blood_delta(df)
    # df = prepare_for_union(df, None)
    return df


def extract_unprocessed_blood(spark_session: SparkSession, resource_path: str):
    unprocessed_blood_spark_schema = convert_cerberus_schema_to_pyspark(unprocessed_blood_validation_schema)

    raw_bloods_delta_header = ",".join(unprocessed_blood_validation_schema.keys())
    df = read_csv_to_pyspark_df(
        spark_session,
        resource_path,
        raw_bloods_delta_header,
        unprocessed_blood_spark_schema,
    )
    return df


def transform_unprocessed_blood_delta(df: DataFrame) -> DataFrame:
    """
    Call functions to process input for blood deltas.
    """
    df = assign_filename_column(df, "unprocessed_blood_source_file")
    df = assign_test_target(df, "antibody_test_target", "unprocessed_blood_source_file")
    df = assign_column_uniform_value(df, "assay_category", 1)

    return df
