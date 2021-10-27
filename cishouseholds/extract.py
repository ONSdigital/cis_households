import subprocess
from datetime import datetime
from typing import List
from typing import Optional
from typing import Union

import pandas as pd
import pyspark.sql.functions as F
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


def get_date_from_filename(filename: str, sep: Optional[str] = "_", format: Optional[str] = "%Y%m%d") -> str:
    """
    Get a date string from a filename of containing a string formatted date
    Parameters
    ----------
    filename
    sep
    format
    """
    try:
        file_date = filename.split(sep)[-1].split(".")[0]
        file_date = datetime.strptime(file_date, format)  # type: ignore
        file_date = file_date.strftime("%Y-%m-%d")  # type: ignore
        return file_date
    except ValueError:
        return str(None)


def list_contents(
    path: str, recursive: Optional[bool] = False, date_from_filename: Optional[bool] = False
) -> DataFrame:
    """
    Read contents of a directory and return the path for each file and
    returns a dataframe of
    Parameters
    ----------
    path : String
    recursive
    """
    spark_session = get_or_create_spark_session()
    command = ["hadoop", "fs", "-ls"]
    if recursive:
        command.append("-R")
    ls = subprocess.Popen([*command, path], stdout=subprocess.PIPE)
    names = ["permission", "id", "owner", "group", "value", "upload_date", "upload_time", "file_path"]
    files = []
    for line in ls.stdout:  # type: ignore
        dic = {}
        f = line.decode("utf-8")
        if "Found" not in f:
            for i, component in enumerate(f.split()):
                dic[names[i]] = component
            dic["filename"] = dic["file_path"].split("/")[-1]
            if date_from_filename:
                file_date = get_date_from_filename(dic["filename"])
                if file_date is not None:
                    dic["upload_date"] = file_date
            files.append(dic)
    return spark_session.createDataFrame(pd.DataFrame(files))


def get_files_by_date(
    path: str, date: Union[str, datetime], selector: str, date2: Optional[Union[str, datetime]] = None
) -> List:
    """
    Get a list of hdfs file paths for a given set of date critera and parent path on
    hdfs
    Parameters
    ----------
    path
        hdfs file path to folder
    date
        reference date for file selection
    selector
        options for selection type: latest, after(date), before(date)
    """
    files = list_contents(path, date_from_filename=True)
    if isinstance(date, datetime):
        date = date.strftime("%Y-%m-%d")
    files = files.withColumn("upload_date", F.to_date("upload_date", "yyyy-MM-dd"))
    if selector == "latest":
        files = files.orderBy("upload_date", "upload_time")
    elif selector == "after":
        files = files.filter(F.col("upload_date") >= (F.lit(date)))
    elif selector == "before":
        files = files.filter(F.col("upload_date") <= (F.lit(date)))
    elif selector == "between":
        if isinstance(date2, datetime):
            date2 = date2.strftime("%Y-%m-%d")
        files = files.filter((F.col("upload_date") >= (F.lit(date))) & (F.col("upload_date") <= (F.lit(date2))))
    return files.select("file_path").rdd.flatMap(lambda x: x).collect()
