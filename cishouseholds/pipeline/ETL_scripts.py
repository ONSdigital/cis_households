from typing import Callable
from typing import List

import pyspark.sql.functions as F
from config import get_config
from config import get_secondary_config

from cishouseholds.edit import cast_columns_from_string
from cishouseholds.edit import convert_columns_to_timestamps
from cishouseholds.edit import rename_column_names
from cishouseholds.edit import update_from_csv_lookup
from cishouseholds.pipeline.load import update_table
from cishouseholds.pyspark_utils import convert_cerberus_schema_to_pyspark
from cishouseholds.pyspark_utils import get_or_create_spark_session


def extract_validate_transform_input_data(
    dataset_name: str,
    id_column: str,
    resource_path: list,
    raw_table_name: str,
    variable_name_map: dict,
    datetime_map: dict,
    validation_schema: dict,
    transformation_functions: List[Callable],
    sep: str = ",",
    cast_to_double_columns_list: list = [],
):
    storage_config = get_config()["storage"]
    csv_location = storage_config["csv_editing_file"]
    filter_config = get_secondary_config(storage_config["filter_config_file"])
    df = extract_input_data(resource_path, validation_schema, sep)
    df = rename_column_names(df, variable_name_map)

    update_table(df, raw_table_name)
    update_table(f"{dataset_name}_rows_extracted", df.filter(F.col(id_column).isin(filter_config[dataset_name])))

    df = df.filter(~F.col(id_column).isin(filter_config[dataset_name]))
    df = update_from_csv_lookup(df=df, csv_filepath=csv_location, dataset_name=dataset_name, id_column=id_column)
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
