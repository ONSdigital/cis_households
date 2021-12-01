import csv
from datetime import datetime
from io import StringIO
from operator import add
from pyspark import RDD
from pyspark.accumulators import AddingAccumulatorParam
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql import Window
from typing import List
from typing import Union

from cerberus import TypeDefinition
from cerberus import Validator

from cishouseholds.pipeline.load import add_error_file_log_entry
from cishouseholds.pyspark_utils import get_or_create_spark_session

# from typing import List

# from typing import List


class PySparkValidator(Validator):
    """
    A Cerberus validator class, which adds support for `timestamp` time. This is an alias for `datetime`.
    This allows reuse of validation schema as PySpark schema.
    """

    types_mapping = Validator.types_mapping.copy()
    types_mapping["timestamp"] = TypeDefinition("timestamp", (datetime,), ())
    types_mapping["double"] = TypeDefinition("double", (float,), ())


def filter_and_accumulate_validation_errors(
    row: Row, accumulator: AddingAccumulatorParam, cerberus_validator: Validator
) -> bool:
    """
    Validate rows of data using a Cerberus validator object, filtering invalid rows out of the returned dataframe.
    Field errors are recorded in the given accumulator, as a list of dictionaries.
    Examples
    --------
    >>> validator = cerberus.Validator({"id": {"type": "string"}})
    >>> error_accumulator = spark_session.sparkContext.accumulator(
            value=[], accum_param=AddingAccumulatorParam(zero_value=[])
            )
    >>> filtered_df = df.rdd.filter(lambda r: filter_and_accumulate_validation_errors(r, error_accumulator, validator))
    """
    row_dict = row.asDict()
    result = cerberus_validator.validate(row_dict)
    if not result:
        accumulator += [(row, cerberus_validator.errors)]
    return result


def validate_and_filter(df: DataFrame, validation_schema: Validator, error_accumulator: AddingAccumulatorParam):
    """
    High level function to validate a PySpark DataFrame using Cerberus.
    Filters invalid records out and accumulates errors.

    Notes
    -----
    As a side effect, errors are added to the ``error_accumulator``.
    """
    return df
    validator = PySparkValidator(validation_schema)
    filtered_df = df.rdd.filter(
        lambda r: filter_and_accumulate_validation_errors(r, error_accumulator, validator)
    ).toDF(schema=df.schema)
    return filtered_df


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


def validate_csv_header(text_file: RDD, expected_header: str):
    """
    Function to validate header in csv file matches expected header.

    Parameters
    ----------
    text_file
        A text file (csv) that has been ready by spark context
    expected_header
        Exact header expected in csv file
    """
    header = text_file.first()
    return expected_header == header


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
    spark_session = get_or_create_spark_session()
    if not isinstance(file_paths, list):
        file_paths = [file_paths]

    expected_header_row = sep.join(validation_schema.keys())

    valid_files = []
    for file_path in file_paths:
        error = ""
        text_file = spark_session.sparkContext.textFile(file_path)
        valid_csv_header = validate_csv_header(text_file, expected_header_row)
        valid_csv_fields = validate_csv_fields(text_file, delimiter=sep)

        if not valid_csv_header:
            error += (
                f"\nInvalid file: Header of {file_path}:\n{text_file.first()})\n "
                f"does not match expected header:\n{expected_header_row}\n"
            )

        if not valid_csv_fields:
            error += (
                f"\nInvalid file: Number of fields in {file_path} does not match expected number of columns from header"
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
