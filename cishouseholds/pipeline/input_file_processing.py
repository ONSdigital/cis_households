from typing import Callable
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from cishouseholds.derive import assign_filename_column
from cishouseholds.edit import cast_columns_from_string
from cishouseholds.edit import convert_columns_to_timestamps
from cishouseholds.edit import rename_column_names
from cishouseholds.edit import update_from_lookup_df
from cishouseholds.extract import extract_lookup_csv
from cishouseholds.pipeline.config import get_config
from cishouseholds.pipeline.config import get_secondary_config
from cishouseholds.pipeline.load import check_table_exists
from cishouseholds.pipeline.load import get_full_table_name
from cishouseholds.pipeline.load import update_table
from cishouseholds.pipeline.validation_schema import csv_lookup_schema
from cishouseholds.pyspark_utils import convert_cerberus_schema_to_pyspark
from cishouseholds.pyspark_utils import get_or_create_spark_session


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

    raw_df = extract_input_data(resource_path, validation_schema, sep)
    raw_df = rename_column_names(raw_df, variable_name_map)
    raw_df = assign_filename_column(raw_df, source_file_column)  # Must be called before update_from_csv_lookup

    df = raw_df
    filtered_df = None
    if include_hadoop_read_write:
        update_table(raw_df, f"raw_{dataset_name}")
        if filter_config is not None and dataset_name in filter_config:
            filter_ids = filter_config[dataset_name]
            filtered_df = df.filter(F.col(id_column).isin(filter_ids))
            update_table(filtered_df, f"{dataset_name}_rows_extracted")

            df = df.filter(~F.col(id_column).isin(filter_ids))

        if csv_location is not None:
            lookup_df = extract_lookup_csv(csv_location, csv_lookup_schema)
            df = update_from_lookup_df(df, lookup_df, id_column=id_column, dataset_name=dataset_name)

    df = convert_columns_to_timestamps(df, datetime_map)
    df = cast_columns_from_string(df, cast_to_double_columns_list, "double")

    for transformation_function in transformation_functions:
        df = transformation_function(df)
    return raw_df, df, filtered_df


def extract_input_data(file_paths: list, validation_schema: dict, sep: str) -> DataFrame:
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


def extract_from_table(table_name: str) -> DataFrame:
    spark_session = get_or_create_spark_session()
    check_table_exists(table_name, raise_if_missing=True)
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
