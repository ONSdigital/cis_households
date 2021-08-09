from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from cishouseholds.validate import validate_csv_fields
from cishouseholds.validate import validate_csv_header


def read_csv_to_pyspark_df(
    spark_session: SparkSession, csv_file_path: str, raw_header_row: str, schema: StructType, **kwargs
) -> DataFrame:
    """
    Validate and read a csv file into a PySpark DataFrame.

    Parameters
    ----------
    csv_file_path
        file to read to dataframe
    raw_header_row
        expected first line of file
    schema
        schema to use for returned dataframe, including desired column names

    Takes keyword arguments from ``spark.read.csv``, for example ``timestampFormat="yyyy-MM-dd HH:mm:ss 'UTC'"``.
    """
    validate_csv_header(csv_file_path, raw_header_row)
    validate_csv_fields(csv_file_path)

    return spark_session.read.csv(csv_file_path, header=True, schema=schema, **kwargs)
