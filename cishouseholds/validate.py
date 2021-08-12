import csv
from datetime import datetime

from cerberus import TypeDefinition
from cerberus import Validator
from pyspark.accumulators import AddingAccumulatorParam
from pyspark.sql import DataFrame
from pyspark.sql import Row


class InvalidFileError(Exception):
    pass


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
    validator = PySparkValidator(validation_schema)
    filtered_df = df.rdd.filter(
        lambda r: filter_and_accumulate_validation_errors(r, error_accumulator, validator)
    ).toDF(schema=df.schema)
    return filtered_df


def validate_csv_fields(csv_file: str, delimiter: str = ","):
    """
    Function to validate the number of fields within records of a csv file.
    Parameters
    ----------
    csv_file
        File path for csv file to be validated
    delimiter
        Delimiter used in csv file, default as ','
    """
    row_errors = []
    with open(csv_file) as f:
        reader = csv.reader(f, delimiter=delimiter)
        n_fields = len(next(reader))

        for line_num, row in enumerate(reader):
            row_fields = len(row)
            if row_fields != n_fields:
                row_errors.append(f"{line_num+1}")

    if row_errors:
        raise InvalidFileError(
            f"Expected number of fields in each row is {n_fields}",
            f"Rows not matching this are: {', '.join(row_errors)}",
        )
    return True


def validate_csv_header(csv_file: str, expected_header: str):
    """
    Function to validate header in csv file matches expected header.
    Parameters
    ----------
    csv_file
        File path for csv file to be validated
    expected_header
        Exact header expected in csv file
    """

    with open(csv_file) as f:
        header = f.readline().strip()

    if expected_header is not None:
        is_match = expected_header == header
        if is_match is False:
            raise InvalidFileError(
                f"Header of csv file {csv_file} does not match expected header",
                f"Actual header: {header}",
                f"Expected header: {expected_header}",
            )
    return True
