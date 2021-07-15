from datetime import datetime

from cerberus import TypeDefinition
from cerberus import Validator
from pyspark.sql import Row

from cishouseholds.pyspark_utils import ListAccumulator


class PysparkValidator(Validator):
    types_mapping = Validator.types_mapping.copy()
    types_mapping["timestamp"] = TypeDefinition("timestamp", (datetime,), ())


def filter_and_accumulate_validation_errors(
    row: Row, accumulator: ListAccumulator, cerberus_validator: Validator
) -> bool:
    """
    Validate rows of data using a Cerberus validator object, filtering invalid rows out of the returned dataframe.
    Field errors are recorded in the given accumulator, as a list of dictionaries.

    Examples
    --------
    >>> error_accumulator = spark_session.sparkContext.accumulator(value=[], accum_param=ListAccumulator())
    >>> filtered_df = df.rdd.filter(lambda r: filter_and_accumulate_validation(r, error_accumulator, validator))
    """
    row_dict = row.asDict()
    result = cerberus_validator.validate(row_dict)
    if not result:
        accumulator += [(row, cerberus_validator.errors)]
    return result
