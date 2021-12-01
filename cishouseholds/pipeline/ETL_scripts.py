from itertools import chain
from typing import Callable
from typing import List

from pyspark.accumulators import AddingAccumulatorParam
from pyspark.sql import SparkSession

from cishouseholds.edit import cast_columns_from_string
from cishouseholds.edit import convert_columns_to_timestamps
from cishouseholds.edit import rename_column_names
from cishouseholds.edit import update_schema_names
from cishouseholds.edit import update_schema_types
from cishouseholds.pyspark_utils import convert_cerberus_schema_to_pyspark
from cishouseholds.pyspark_utils import get_or_create_spark_session
from cishouseholds.validate import validate_and_filter


def extract_validate_transform_input_data(
    resource_path: list,
    variable_name_map: dict,
    datetime_map: dict,
    validation_schema: dict,
    transformation_functions: List[Callable],
    sep: str = ",",
    cast_to_double_columns_list: list = [],
):
    spark_session = get_or_create_spark_session()
    df = extract_input_data(spark_session, resource_path, validation_schema, sep)
    df = rename_column_names(df, variable_name_map)
    df = convert_columns_to_timestamps(df, datetime_map)
    _validation_schema = update_schema_names(validation_schema, variable_name_map)
    datetime_map_list = list(chain(*list(datetime_map.values())))
    _validation_schema = update_schema_types(_validation_schema, datetime_map_list, {"type": "timestamp"})
    df = cast_columns_from_string(df, cast_to_double_columns_list, "double")
    _validation_schema = update_schema_types(_validation_schema, cast_to_double_columns_list, {"type": "double"})

    error_accumulator = spark_session.sparkContext.accumulator(
        value=[], accum_param=AddingAccumulatorParam(zero_value=[])
    )

    df = validate_and_filter(df, _validation_schema, error_accumulator)
    for transformation_function in transformation_functions:
        df = transformation_function(df)
    return df


def extract_input_data(spark_session: SparkSession, file_paths: list, validation_schema: dict, sep: str):
    spark_schema = convert_cerberus_schema_to_pyspark(validation_schema)
    return spark_session.read.csv(
        file_paths,
        header=True,
        schema=spark_schema,
        ignoreLeadingWhiteSpace=True,
        ignoreTrailingWhiteSpace=True,
        sep=sep,
    )
