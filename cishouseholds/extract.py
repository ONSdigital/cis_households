from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from cishouseholds.pyspark_utils import get_or_create_spark_session
from cishouseholds.validate import validate_csv_fields
from cishouseholds.validate import validate_csv_header


class InvalidFileError(Exception):
    pass


def read_csv_to_pyspark_df(
    spark_session: SparkSession, csv_file_path: str, expected_raw_header_row: str, schema: StructType, **kwargs
) -> DataFrame:
    """
    Validate and read a csv file into a PySpark DataFrame.

    Parameters
    ----------
    csv_file_path
        file to read to dataframe
    expected_raw_header_row
        expected first line of file
    schema
        schema to use for returned dataframe, including desired column names

    Takes keyword arguments from ``pyspark.sql.DataFrameReader.csv``,
    for example ``timestampFormat="yyyy-MM-dd HH:mm:ss 'UTC'"``.
    """
    spark_session = get_or_create_spark_session()
    text_file = spark_session.sparkContext.textFile(csv_file_path)
    validate_csv_header(text_file, expected_raw_header_row)
    validate_csv_fields(text_file)

    # if not is_valid_header:
    #     raise InvalidFileError(
    #         f"Header of csv file {csv_file_path} does not match expected header",
    #         f"Actual header:     {header}",
    #         f"Expected header:   {expected_header}",
    #     )

    return spark_session.read.csv(
        csv_file_path, header=True, schema=schema, ignoreLeadingWhiteSpace=True, ignoreTrailingWhiteSpace=True, **kwargs
    )
