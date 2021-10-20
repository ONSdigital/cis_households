import re
import subprocess
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from cishouseholds.pyspark_utils import get_or_create_spark_session
from cishouseholds.validate import validate_csv_fields
from cishouseholds.validate import validate_csv_header


class InvalidFileError(Exception):
    pass


def read_csv_to_pyspark_df(
    spark_session: SparkSession,
    csv_file_path: str,
    expected_raw_header_row: str,
    schema: StructType,
    sep: str = ",",
    **kwargs,
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
    csv_header = validate_csv_header(text_file, expected_raw_header_row)
    csv_fields = validate_csv_fields(text_file, delimiter=sep)

    if not csv_header:
        raise InvalidFileError(f"Header of {csv_file_path} does not match expected header: {expected_raw_header_row}")

    if not csv_fields:
        raise InvalidFileError(
            f"Number of fields in {csv_file_path} does not match expected number of columns from header"
        )

    return spark_session.read.csv(
        csv_file_path,
        header=True,
        schema=schema,
        ignoreLeadingWhiteSpace=True,
        ignoreTrailingWhiteSpace=True,
        sep=sep,
        **kwargs,
    )


def list_contents(
    path: str,
    level: Optional[str] = "path",
    recursive: Optional[bool] = False,
):
    """
    Read contents of a directory and return the path for each file, alternatively
    returning only full path or all metadata.
    Parameters
    ----------
    path : String
    level : String
    The default is 'path' of the object, 'metadata' will all metadata and 'filename' only the
    name of all objects in the dir.    recursive : Boolean (opt)
    Note
    ----
    Accepts wildcard/ glob pattern filtering syntax, e.g. the below will only return
    files names that are .gz files from the 2010's (201[0,1,2,..,9])

    e.g.    >>> list_contents('/dapsen/landing_zone/retailer/historic/201*/v1/*.gz', 'filename')
    """
    if level not in [None, "path", "filename", "metadata"]:
        raise ValueError("paths_or_name_only only accepts 'path' or 'filename'" "as an argument")
    command = ["hadoop", "fs", "-ls"]
    if recursive:
        command.append("-R")
        ls = subprocess.Popen([*command, path], stdout=subprocess.PIPE)
        files = []
        for line in ls.stdout:  # type: ignore
            f = line.decode("utf-8")
            if "Found" not in f:
                metadata, file_path = re.split(r"[ ](?=/)", f)
                files.append(metadata.split() + [file_path[:-1]])
                if level == "metadata":
                    return files
                elif level == "path":
                    return [file[-1] for file in files]
                elif level == "filename":
                    return [file.split("/")[-1] for file in [file[-1] for file in files]]
    return None
