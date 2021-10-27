from itertools import chain

from pyspark.accumulators import AddingAccumulatorParam
from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession

from cishouseholds.derive import assign_column_uniform_value
from cishouseholds.derive import assign_filename_column
from cishouseholds.derive import assign_test_target
from cishouseholds.derive import substring_column
from cishouseholds.edit import convert_columns_to_timestamps
from cishouseholds.edit import rename_column_names
from cishouseholds.edit import update_schema_names
from cishouseholds.edit import update_schema_types
from cishouseholds.extract import read_csv_to_pyspark_df
from cishouseholds.pipeline.input_variable_names import blood_variable_name_map
from cishouseholds.pipeline.load import update_table_and_log_source_files
from cishouseholds.pipeline.pipeline_stages import register_pipeline_stage
from cishouseholds.pipeline.timestamp_map import blood_datetime_map
from cishouseholds.pipeline.validation_schema import blood_validation_schema
from cishouseholds.pyspark_utils import convert_cerberus_schema_to_pyspark
from cishouseholds.pyspark_utils import get_or_create_spark_session
from cishouseholds.validate import validate_and_filter

# from cishouseholds.compare import prepare_for_union


@register_pipeline_stage("blood_delta_ETL")
def blood_delta_ETL(resource_path: str):
    df = extract_validate_transform_blood_delta(resource_path)
    update_table_and_log_source_files(df, "transformed_blood_test_data", "blood_test_source_file")
    return df


def extract_validate_transform_blood_delta(resource_path: str):
    spark_session = get_or_create_spark_session()

    df = extract_blood_delta(spark_session, resource_path)
    df = rename_column_names(df, blood_variable_name_map)
    df = convert_columns_to_timestamps(df, blood_datetime_map)

    _blood_validation_schema = update_schema_names(blood_validation_schema, blood_variable_name_map)

    df = convert_columns_to_timestamps(df, blood_datetime_map)
    blood_datetime_map_list = list(chain(*list(blood_datetime_map.values())))
    _blood_validation_schema = update_schema_types(
        _blood_validation_schema, blood_datetime_map_list, {"type": "timestamp"}
    )

    error_accumulator = spark_session.sparkContext.accumulator(
        value=[], accum_param=AddingAccumulatorParam(zero_value=[])
    )
    df = validate_and_filter(df, _blood_validation_schema, error_accumulator)
    df = transform_blood_delta(df)
    # df = prepare_for_union(df, None)
    return df


def extract_blood_delta(spark_session: SparkSession, resource_path: str):
    blood_spark_schema = convert_cerberus_schema_to_pyspark(blood_validation_schema)

    raw_bloods_delta_header = ",".join(blood_validation_schema.keys())
    df = read_csv_to_pyspark_df(
        spark_session,
        resource_path,
        raw_bloods_delta_header,
        blood_spark_schema,
    )
    return df


def transform_blood_delta(df: DataFrame) -> DataFrame:
    """
    Call functions to process input for blood deltas.
    """
    df = assign_filename_column(df, "blood_test_source_file")
    df = assign_test_target(df, "antibody_test_target", "blood_test_source_file")
    df = substring_column(df, "plate", "antibody_test_plate_id", 5, 5)
    df = assign_column_uniform_value(df, "assay_category", 1)

    return df
