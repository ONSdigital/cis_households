from itertools import chain
from typing import List
from typing import Mapping

import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def convert_null_if_not_in_list(df: DataFrame, column_name: str, options_list: List[str]) -> DataFrame:
    """
    Convert column values to null if the entry is no present in provided list
    Parameters
    ----------
    df
    column_name
    options_list
    """
    df = df.withColumn(
        column_name, F.when((F.col(column_name).isin(*options_list)), F.col(column_name)).otherwise(None)
    )

    return df


def convert_barcode_null_if_zero(df: DataFrame, barcode_column_name: str):
    """
    Converts barcode to null if numeric characters are all 0 otherwise performs no change
    Parameters
    ----------
    df
    barcode_column_name
    """
    df = df.withColumn(
        barcode_column_name,
        F.when(F.substring(barcode_column_name, 4, 999) == "0" * (F.length(barcode_column_name) - 3), None).otherwise(
            F.col(barcode_column_name)
        ),
    )

    return df


def convert_columns_to_timestamps(df: DataFrame, column_format_map: dict) -> DataFrame:
    """
    Convert string columns to timestamp given format.
    Parameters
    ----------
    df
    column_format_map
        format of datetime string and associated list of column names to which it applies
    """
    for format, columns_list in column_format_map.items():
        for column_name in columns_list:
            df = df.withColumn(column_name, F.to_timestamp(F.col(column_name), format=format))
    return df


def update_schema_types(schema: dict, column_names: list, new_type: dict):
    """
    Update entries within schema dictionary to reflect a common change across all rows in list (column_names)
    Parameters
    ----------
    schema
    column_names
        list of names of keys within schema to assign new type to
    new_type
        type dictionary to update the schame entry to
    """
    for column_name in column_names:
        schema[column_name] = new_type
    return schema


def format_string_upper_and_clean(df: DataFrame, column_name_to_assign: str) -> str:
    """
    Remove all instances of whitespace before and after a string field including all duplicate spaces
    along with dots (.) aswell
    Parameters
    ----------
    df
    column_name_to_assign
    """
    df = df.withColumn(
        column_name_to_assign,
        F.upper(F.ltrim(F.rtrim(F.regexp_replace(column_name_to_assign, "\s+", " ")))),  # noqa W605
    )
    df = df.withColumn(
        column_name_to_assign,
        F.when(
            F.substring(column_name_to_assign, -1, 1) == ".",
            F.rtrim(F.col(column_name_to_assign).substr(F.lit(1), F.length(column_name_to_assign) - 1)),
        ).otherwise(F.col(column_name_to_assign)),
    )

    return df


def rename_column_names(df: DataFrame, variable_name_map: dict) -> DataFrame:
    """
    Rename column names.
    Parameters
    ----------
    df
    variable_name_map
        map of current column names to new names
    """
    cleaned_columns = [variable_name_map[old_column_name] for old_column_name in df.columns]
    return df.toDF(*cleaned_columns)


def assign_from_map(df: DataFrame, column_name_to_assign: str, reference_column: str, mapper: Mapping) -> DataFrame:
    """
    Assign column with values based on a dictionary map of reference_column.
    From households_aggregate_processes.xlsx, edit number 1.
    Parameters
    ----------
    df
    column_name_to_assign
        Name of column to be assigned
    reference_column
        Name of column of TimeStamp type to be converted
    mapper
        Dictionary of key value pairs to edit values
    Return
    ------
    pyspark.sql.DataFrame
    Notes
    -----
    Function works if key and value are of the same type and there is a missing key in the mapper
    If types are the same, the missing keys will be replaced with the reference column value/
    If types are not the same, the missing keys will be given as NULLS
    If key and value are of a different type and there is a missing key in the mapper,
        then the type is not converted.
    """
    key_types = set([type(key) for key in mapper.keys()])
    value_types = set([type(values) for values in mapper.values()])
    assert len(key_types) == 1, f"all map keys must be the same type, they are {key_types}"
    assert len(value_types) == 1, f"all map values must be the same type, they are {value_types}"

    mapping_expr = F.create_map([F.lit(x) for x in chain(*mapper.items())])

    if key_types == value_types:
        return df.withColumn(
            column_name_to_assign, F.coalesce(mapping_expr[F.col(reference_column)], F.col(reference_column))
        )
    else:
        return df.withColumn(column_name_to_assign, mapping_expr[F.col(reference_column)])


def assign_null_if_insufficient(
    df: DataFrame, column_name_to_assign: str, first_reference_column: str, second_reference_column: str
):
    """
    Assign a null values based on bloods insufficient logic, where two columns both have specified values.
    From households_aggregate_processes.xlsx, edit number 2.
    Parameters
    ----------
    df
    column_name_to_assign
        Name of column to be assigned
    first_reference_column
        First column to check value of for null condition
    second_reference_column
        Second column to check value of for null condition
    Return
    ------
    pyspark.sql.DataFrame
    """
    return df.withColumn(
        column_name_to_assign,
        F.when(
            (F.col(first_reference_column) == 0) & (F.col(second_reference_column) == "Insufficient sample"), None
        ).otherwise(F.col(first_reference_column)),
    )


def edit_swab_results_single(
    df: DataFrame, gene_result_classification: str, gene_result_value: str, overall_result_classification: str
) -> DataFrame:
    """
    The objective of this function is to edit/correct the gene_result_classification from Positive to Negative or 1 to 0
        in case gene_result_value is 0.0 or lower and overall_result_classification is Positive or 1.

    Parameters
    ----------
    df
    gene_result_classification
    gene_result_value
        column name that consists of float values
    overall_result_classification
    """
    return df.withColumn(
        gene_result_classification,
        F.when(
            # boolean logic:
            (F.col(gene_result_classification) == "Positive")
            & (F.col(gene_result_value) <= 0.0)
            & (F.col(overall_result_classification) == "Positive"),
            "Negative"
            # if boolean condition not met, keep the same value.
        ).otherwise(F.col(gene_result_classification)),
    )
