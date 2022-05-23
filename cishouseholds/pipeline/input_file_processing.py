from typing import Callable
from typing import List
from typing import Union

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from cishouseholds.derive import assign_filename_column
from cishouseholds.edit import cast_columns_from_string
from cishouseholds.edit import convert_columns_to_timestamps
from cishouseholds.edit import rename_column_names
from cishouseholds.edit import update_from_lookup_df
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
    extract and validate a csv lookup file from path with validation_schema
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
    variable_name_map: dict = None,
    sep: str = ",",
    cast_to_double_columns_list: list = [],
    include_hadoop_read_write: bool = False,
    dataset_version: str = None,
):
    if include_hadoop_read_write:
        storage_config = get_config()["storage"]
        record_editing_config_path = storage_config["record_editing_config_file"]
        extraction_config = get_secondary_config(storage_config["record_extraction_config_file"])

    df = extract_input_data(resource_path, validation_schema, sep)
    if variable_name_map is not None:
        df = rename_column_names(df, variable_name_map)

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
            editing_lookup_df = extract_lookup_csv(record_editing_config_path, validation_schemas["csv_lookup_schema"])
            df = update_from_lookup_df(df, editing_lookup_df, id_column=id_column, dataset_name=dataset_name)

    df = convert_columns_to_timestamps(df, datetime_map)
    df = cast_columns_from_string(df, cast_to_double_columns_list, "double")

    for transformation_function in transformation_functions:
        df = transformation_function(df)
    return df


def extract_input_data(file_paths: Union[List[str], str], validation_schema: Union[dict, None], sep: str) -> DataFrame:
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
