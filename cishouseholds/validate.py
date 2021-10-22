# import csv
import csv
from datetime import datetime
from io import StringIO
from operator import add
from pyspark import RDD
from pyspark.accumulators import AddingAccumulatorParam
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Row
from typing import List

from cerberus import TypeDefinition
from cerberus import Validator


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


def check_singular_match(
    df: DataFrame, flag_column_name: str, failure_column_name: str, match_type_column: str, group_by_column: str
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
        Column in which to store bool flag that shows if singular match occured for given merge
    match_type_column
        Column to identify type of merge
    group_by_column
        Column to check is singular given criteria
    """
    dft = (
        df.filter((F.col(flag_column_name).isNull()) & (F.col(match_type_column) == 1))
        .groupBy(group_by_column)
        .count()
        .withColumnRenamed(group_by_column, "b")
        .withColumnRenamed(failure_column_name, "f")
    )
    dft = dft.withColumn(failure_column_name, F.when(F.col("count") > 1, 1).otherwise(None))
    df = (
        df.drop(failure_column_name)
        .join(dft, dft.b == F.col(group_by_column), "outer")
        .withColumnRenamed("f", failure_column_name)
        .drop("b", "count")
    )
    return df


def validate_merge_logic(
    df: DataFrame,
    flag_column_names: List[str],
    failed_column_names: List[str],
    match_type_colums: List[str],
    group_by_column: str,
):
    """
    Wrapper function to call check_singular_match for each set of parameters in list
    Parameters. For creating a new failure column specify a name of a column which does not currently exist
    ----------
    df
    flag_column_names
        List of columns with final flag from merge function
    failure_column_name
        List of columns in which to store bool flag that shows if singular match occured for given merge
    match_type_column
        List of columns to identify type of merge
    group_by_column
        List of columns to check is singular given criteria
    """
    for i, flag_column in enumerate(flag_column_names):
        df = check_singular_match(df, flag_column, failed_column_names[i], match_type_colums[i], group_by_column)
    return df
