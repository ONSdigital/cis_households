from itertools import chain
from typing import Callable

from pyspark.accumulators import AddingAccumulatorParam
from pyspark.sql import SparkSession

from cishouseholds.edit import convert_columns_to_timestamps
from cishouseholds.edit import rename_column_names
from cishouseholds.edit import update_schema_names
from cishouseholds.edit import update_schema_types
from cishouseholds.extract import read_csv_to_pyspark_df
from cishouseholds.pyspark_utils import convert_cerberus_schema_to_pyspark
from cishouseholds.pyspark_utils import get_or_create_spark_session
from cishouseholds.validate import validate_and_filter

# from cishouseholds.derive import assign_work_person_facing_now


def extract_validate_transform_input_data(
    resource_path: str,
    variable_name_map: dict,
    datetime_map: dict,
    validation_schema: dict,
    transformation_function: Callable,
):
    spark_session = get_or_create_spark_session()
    df = extract_input_data(spark_session, resource_path, validation_schema)
    df = rename_column_names(df, variable_name_map)
    df = convert_columns_to_timestamps(df, datetime_map)
    _validation_schema = update_schema_names(validation_schema, variable_name_map)
    datetime_map_list = list(chain(*list(datetime_map.values())))
    _validation_schema = update_schema_types(_validation_schema, datetime_map_list, {"type": "timestamp"})

    error_accumulator = spark_session.sparkContext.accumulator(
        value=[], accum_param=AddingAccumulatorParam(zero_value=[])
    )

    df = validate_and_filter(df, _validation_schema, error_accumulator)
    df = transformation_function(df)
    return df


def extract_input_data(spark_session: SparkSession, resource_path: str, validation_schema: dict):
    spark_schema = convert_cerberus_schema_to_pyspark(validation_schema)
    raw_data_header = "|".join(validation_schema.keys())
    df = read_csv_to_pyspark_df(spark_session, resource_path, raw_data_header, spark_schema, sep="|")
    return df
