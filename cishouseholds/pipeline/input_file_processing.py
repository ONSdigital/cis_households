from pathlib import Path
from typing import Callable
from typing import List
from typing import Union

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from cishouseholds.derive import assign_filename_column
from cishouseholds.edit import cast_columns_from_string
from cishouseholds.edit import convert_columns_to_timestamps
from cishouseholds.edit import rename_column_names
from cishouseholds.edit import update_from_lookup_df
from cishouseholds.hdfs_utils import read_file_to_string
from cishouseholds.merge import union_multiple_tables
from cishouseholds.phm.json_decode import decode_phm_json
from cishouseholds.pipeline.config import get_config
from cishouseholds.pipeline.config import get_secondary_config
from cishouseholds.pipeline.load import update_table
from cishouseholds.pipeline.validation_schema import validation_schemas
from cishouseholds.pyspark_utils import convert_cerberus_schema_to_pyspark
from cishouseholds.pyspark_utils import get_or_create_spark_session
from cishouseholds.validate import validate_files


class InvalidFileError(Exception):
    pass


def extract_lookup_csv(
    lookup_file_path: str, validation_schema: dict, column_name_map: dict = None, drop_not_found: bool = False
):
    """
    Extract and validate a csv lookup file from path with validation_schema for column data types
    whilst applying a map to rename columns from the csv file to the dataframe.

    Parameters
    ----------
    validation_schema
        schema to map column names to required data type, if type not compatible value set to null
    column_name_map
        map of current column names to new pipeline names
    drop_not_found
        optional boolean flag to drop any columns not on the column name map from the input data
    """
    valid_files = validate_files(lookup_file_path, validation_schema)  # type: ignore
    if not valid_files:
        raise InvalidFileError(f"Lookup csv file {lookup_file_path} is not valid.")
    df = extract_input_data(lookup_file_path, validation_schema, sep=",")  # type: ignore
    if column_name_map is None:
        return df
    if drop_not_found:
        df = df.select(*column_name_map.keys())
    df = rename_column_names(df, column_name_map)
    return df


def extract_validate_transform_input_data(
    dataset_name: str,
    id_column: str,
    resource_path: list,
    datetime_map: dict,
    validation_schema: dict,
    transformation_functions: List[Callable],
    source_file_column: str,
    write_mode: str,
    column_name_map: dict = None,
    sep: str = ",",
    cast_to_double_columns_list: list = [],
    include_hadoop_read_write: bool = False,
    dataset_version: str = None,
):
    """
    Wraps the extract input data function reading a set of csv files into a single dataframe,
    logs the source file name, creates and saves a "raw" table that acts as an unaltered archive of the input
    table. After this any manual filtering and editing steps are applied to the table before the correct
    column timestamp formats are applied to the data and the main transformation function is finally executed.

    Parameters
    ----------
    dataset_name
        name for the group of files being read in
    id_column
        column on the table which acts as unique id for the particular dataset
    resource_path
        hdfs file pattern to the input data
    datetime_map
        dictionary of datetime columns to format mapping
    validation_schema
        schema to map column names to required data type
    transformation_functions: List[Callable]
        list of functions in order of which they need to be applied to the input dataframe
    source_file_column
        column name in which to store the source file name
    write_mode
        mode to adress the hdfs table (overwrite / append)
    column_name_map
        map of current column names to new pipeline names
    sep
        optional value seperator used in the csv file
        e.g. | or ,
    cast_to_double_columns_list
        optional list of column names that must be casted to double type columns
    include_hadoop_read_write
        optional boolean toggle to denote if hdfs operations should be used
    dataset_version
        optional integer to denote the voyager version of the survey file
    """
    if include_hadoop_read_write:
        storage_config = get_config()["storage"]
        record_editing_config_path = storage_config["record_editing_config_file"]
        extraction_config = get_secondary_config(storage_config["record_extraction_config_file"])

    df = extract_input_data(resource_path, validation_schema, sep, source_file_column)
    if column_name_map is not None:
        df = rename_column_names(df, column_name_map)

    df = assign_filename_column(df, source_file_column)  # Must be called before update_from_lookup_df
    dataset_version = "" if dataset_version is None else "_" + dataset_version
    if include_hadoop_read_write:
        update_table(df, f"raw_{dataset_name}{dataset_version}", write_mode)
        filter_ids = []
        if extraction_config is not None and dataset_name in extraction_config:
            filter_ids = extraction_config[dataset_name]
        filtered_df = df.filter(F.col(id_column).isin(filter_ids))
        update_table(filtered_df, f"extracted_{dataset_name}{dataset_version}", write_mode)
        df = df.filter(~F.col(id_column).isin(filter_ids))

        if record_editing_config_path is not None:
            editing_lookup_df = extract_lookup_csv(
                record_editing_config_path, validation_schemas["csv_lookup_schema_extended"]
            )
            df = update_from_lookup_df(df, editing_lookup_df, dataset_name=dataset_name)

    df = convert_columns_to_timestamps(df, datetime_map)
    df = cast_columns_from_string(df, cast_to_double_columns_list, "double")

    for transformation_function in transformation_functions:
        df = transformation_function(df)
    return df


def extract_input_data(
    file_paths: Union[List[str], str],
    validation_schema: Union[dict, None],
    sep: str = ",",
    source_file_column: str = "",
) -> DataFrame:
    """
    Converts a validation schema in cerberus format into a pyspsark readable schema and uses it to read
    a csv filepath into a dataframe.

    Parameters
    ----------
    file_paths
        file path or pattern to extract any number of matching files into single dataframe
    validation_schema
        schema to map column names to required data type
    """
    spark_session = get_or_create_spark_session()
    spark_schema = convert_cerberus_schema_to_pyspark(validation_schema) if validation_schema is not None else None
    file_paths = [file_paths] if type(file_paths) == str else file_paths
    csv_file_paths = [p for p in file_paths if Path(p).suffix in [".txt", ".csv"]]
    xl_file_paths = [p for p in file_paths if Path(p).suffix == ".xlsx"]
    json_file_paths = [p for p in file_paths if Path(p).suffix == ".json"]
    df = None
    if csv_file_paths:
        if validation_schema:
            valid_file_paths = validate_files(csv_file_paths, validation_schema, sep=sep)
        if not valid_file_paths:
            print(f"        - No valid files found in: {csv_file_paths}.")  # functional
            return {"status": "Error"}
        df = spark_session.read.csv(
            csv_file_paths,
            header=True,
            schema=spark_schema,
            ignoreLeadingWhiteSpace=True,
            ignoreTrailingWhiteSpace=True,
            sep=sep,
        )
    if xl_file_paths:
        spark = get_or_create_spark_session()
        dfs = [
            spark.createDataFrame(
                data=pd.read_excel(read_file_to_string(file, True), engine="openpyxl"), schema=spark_schema
            ).withColumn(source_file_column, (F.regexp_replace(F.lit(file), r"(?<=:\/{2})(\w+|\d+)(?=\/{1})", "")))
            for file in xl_file_paths
        ]
        if df is None:
            df = union_multiple_tables(dfs)
        else:
            df = union_multiple_tables([df, *dfs])
    if json_file_paths:
        dfs = []
        data_strings = [read_file_to_string(file, True) for file in json_file_paths]
        for data_string in data_strings:
            answers, list_items = decode_phm_json(data_string)
            dfs.append(spark_session.createDataFrame(data=[tuple(answers.values())], schema=spark_schema))
        if df is None:
            df = union_multiple_tables(dfs)
        else:
            df = union_multiple_tables([df, *dfs])
    return df
