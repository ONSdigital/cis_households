import csv
import inspect
from io import StringIO
from operator import add
from typing import Dict
from typing import List
from typing import Union

from pyspark import RDD
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window

from cishouseholds.pipeline.load import add_error_file_log_entry
from cishouseholds.pyspark_utils import get_or_create_spark_session


def validate_csv_fields(text_file: RDD, delimiter: str = ","):
    """
    Function to validate the number of fields within records of a csv file.
    Parameters
    ----------
    text_file
        A text file (csv) that has been ready by spark context
    delimiter
        Delimiter used in csv file, default as ','
    """

    def count_fields_in_row(delimiter, row):
        f = StringIO(row)
        reader = csv.reader(f, delimiter=delimiter)
        n_fields = len(next(reader))
        return n_fields

    header = text_file.first()
    number_of_columns = count_fields_in_row(delimiter, header)
    error_count = text_file.map(lambda row: count_fields_in_row(delimiter, row) != number_of_columns).reduce(add)
    return True if error_count == 0 else False


def validate_csv_header(text_file: RDD, expected_header: List[str], delimiter: str = ","):
    """
    Function to validate header in csv file matches expected header.

    Parameters
    ----------
    text_file
        A text file (csv) that has been read by spark context
    expected_header
        Exact header expected in csv file
    """
    actual_header = text_file.first()
    buffer = StringIO(actual_header)
    reader = csv.reader(buffer, delimiter=delimiter)
    actual_header = next(reader)
    return expected_header == actual_header


def validate_files(file_paths: Union[str, list], validation_schema: dict, sep: str = ","):
    """
    Validate the header and field count of one or more CSV files on HDFS.

    Parameters
    ----------
    file_paths
        one or more paths to files to validate
    validation_schema
        dictionary with ordered keys containing expected column names
    sep
        file separator
    """
    if file_paths is None or file_paths == "":
        raise FileNotFoundError("No file path specified")
    spark_session = get_or_create_spark_session()
    if file_paths is None or file_paths in ["", []]:
        raise FileNotFoundError("No file path specified")
    if not isinstance(file_paths, list):
        file_paths = [file_paths]

    expected_header_row = list(validation_schema.keys())

    valid_files = []
    for file_path in file_paths:
        error = ""
        text_file = spark_session.sparkContext.textFile(file_path)
        valid_csv_header = validate_csv_header(text_file, expected_header_row, delimiter=sep)
        valid_csv_fields = validate_csv_fields(text_file, delimiter=sep)

        if not valid_csv_header:
            actual_header = text_file.first()
            buffer = StringIO(actual_header)
            reader = csv.reader(buffer, delimiter=sep)
            actual_header = next(reader)
            expected_header = list(validation_schema.keys())
            error += (
                f"Invalid file header:{file_path}\n"
                f"Expected:     {expected_header}\n"
                f"Actual:       {actual_header}\n"
                f"Missing:      {set(expected_header) - set(actual_header)}\n"
                f"Additional:   {set(actual_header) - set(expected_header)}\n"
            )

        if not valid_csv_fields:
            error += (
                f"\nInvalid file: Number of fields in {file_path} "
                "row(s) does not match expected number of columns from header"
            )

        if error != "":
            print(error)  # functional
            add_error_file_log_entry(file_path, error)
        else:
            valid_files.append(file_path)
    return valid_files


def check_singular_match(
    df: DataFrame,
    drop_flag_column_name: str,
    failure_column_name: str,
    group_by_columns: List[str],
    existing_failure_column: str = None,
):
    """
    Given a set of columns related to the final drop flag of a given merge function on the complete
    (merged) dataframe produce an indication column (failure column) which stipulates whether the
    merge function has returned a unique match
    Parameters
    ----------
    df
    flag_column_name
        Column with final flag from merge function
    failure_column_name
        Column in which to store bool flag that shows if singular match occurred for given merge
    match_type_column
        Column to identify type of merge
    group_by_column
        Column to check is singular given criteria
    """
    if type(group_by_columns) != list:
        group_by_columns = [group_by_columns]  # type: ignore
    window = Window.partitionBy(*group_by_columns, drop_flag_column_name)
    df = df.withColumn("TOTAL", F.sum(F.lit(1)).over(window))

    df = df.withColumn(
        failure_column_name, F.when((F.col("total") > 1) & (F.col(drop_flag_column_name).isNull()), 1).otherwise(None)
    )
    if existing_failure_column is not None:
        df = df.withColumn(
            failure_column_name, F.when(F.col(existing_failure_column) == 1, 1).otherwise(F.col(failure_column_name))
        )
    return df.drop("TOTAL")


class ConfigError(Exception):
    pass


def upfront_key_value_parameters_validation(all_function_dict: Dict, config_file_arguments: List):
    """
    Checks that there's a valid input in the pipeline_config.yaml for every stage
    input argument.

    Parameters
    ----------
    all_function_dict: dictionary of all functions name and function object in pipeline_stages.py
    pipeline_stage_list: from the config file all the functions that have been set up to run.
    """
    error_msg = ""
    for function_run_dict in config_file_arguments:
        function_name = function_run_dict.pop("function")
        function_run_list = [x for x in function_run_dict.keys()]

        input_arguments_needed = [
            arg
            for arg in inspect.getargspec(all_function_dict[function_name]).args
            if "=" not in str(inspect.signature(all_function_dict[function_name]).parameters[arg])
        ]

        if not (set(function_run_list) == set(input_arguments_needed)):
            error_msg += f"\nThe following argument(s) for {function_name} stage \n"

            list_not_passed_arg = [x for x in input_arguments_needed if x not in function_run_list]
            list_of_unrecognised_arg = [x for x in function_run_list if x not in input_arguments_needed]

            if list_not_passed_arg:
                error_msg += f""" - are not passed in the config file: {', '.join(list_not_passed_arg)}.\n"""

            if list_of_unrecognised_arg:
                error_msg += f""" - are not recognised as input arguments: {', '.join(list_of_unrecognised_arg)}.\n"""
    if error_msg:
        raise ConfigError(error_msg)
