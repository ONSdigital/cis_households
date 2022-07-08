import csv
import re
from io import StringIO
from operator import add
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


def normalise_schema(file_path: str, reference_validation_schema: dict, regex_schema: dict):
    """
    Use a series of regex patterns mapped to correct column names to build an individual schema
    for a given csv input file that has varied headings across a group of similar files.
    """
    spark_session = get_or_create_spark_session()

    file = spark_session.sparkContext.textFile(file_path)
    actual_header = file.first()
    buffer = StringIO(actual_header)
    reader = csv.reader(buffer, delimiter=",")
    actual_header = next(reader)
    validation_schema = {}
    column_name_map = {}
    dont_drop_list = []
    if actual_header != list(reference_validation_schema.keys()):
        for actual_col in actual_header:
            validation_schema[actual_col] = {"type": "string"}
            for regex, normalised_column in regex_schema.items():
                if re.search(rf"{regex}", actual_col):
                    validation_schema[actual_col] = reference_validation_schema[normalised_column]
                    column_name_map[actual_col] = normalised_column
                    dont_drop_list.append(actual_col)
                    break
        if set(column_name_map.values()) == set(reference_validation_schema.keys()):
            return None, validation_schema, column_name_map, [col for col in actual_header if col not in dont_drop_list]
        else:
            error_message = (
                f"{file_path} is invalid as header({actual_header} contained unrecognisable columns"  # functional
            )
            return error_message, {}, {}, []
    return None, reference_validation_schema, {}, []


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


def check_lookup_table_joined_columns_unique(df, join_column_list, name_of_df):
    duplicate_key_rows_df = df.groupBy(*join_column_list).count().filter("count > 1").drop("count")
    if duplicate_key_rows_df.count() > 0:
        raise ValueError(
            f"The lookup dataframe {name_of_df} has entried with duplicate join keys ({', '.join(join_column_list)})."
            f"Duplicate rows: \n{duplicate_key_rows_df.toPandas()}"
        )
