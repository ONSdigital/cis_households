from typing import Callable
from typing import List

from cishouseholds.edit import cast_columns_from_string
from cishouseholds.edit import convert_columns_to_timestamps
from cishouseholds.edit import rename_column_names
from cishouseholds.pyspark_utils import convert_cerberus_schema_to_pyspark
from cishouseholds.pyspark_utils import get_or_create_spark_session


def extract_validate_transform_input_data(
    resource_path: list,
    variable_name_map: dict,
    datetime_map: dict,
    validation_schema: dict,
    transformation_functions: List[Callable],
    sep: str = ",",
    cast_to_double_columns_list: list = [],
):
    df = extract_input_data(resource_path, validation_schema, sep)
    df = rename_column_names(df, variable_name_map)
    df = convert_columns_to_timestamps(df, datetime_map)
    df = cast_columns_from_string(df, cast_to_double_columns_list, "double")

    for transformation_function in transformation_functions:
        df = transformation_function(df)
    return df


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
