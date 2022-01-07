import subprocess
from datetime import datetime
from typing import List
from typing import Optional
from typing import Union

import pandas as pd
from pyspark.sql import DataFrame

from cishouseholds.pipeline.config import get_config
from cishouseholds.pipeline.load import check_table_exists
from cishouseholds.pipeline.load import extract_from_table
from cishouseholds.pyspark_utils import column_to_list
from cishouseholds.pyspark_utils import get_or_create_spark_session


class InvalidFileError(Exception):
    pass


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
        files.append(dic)
    df = pd.DataFrame(files)
    if date_from_filename:
        df["upload_date"] = df["filename"].str.extract((r"(\d{8})(_\d{4})?(.csv)"), expand=False)
        df["upload_date"] = pd.to_datetime(df["upload_date"], errors="coerce", format="%Y%m%d")

    return df


def get_files_by_date(
    path: str,
    latest_only: bool = False,
    start_date: Optional[Union[str, datetime]] = None,
    end_date: Optional[Union[str, datetime]] = None,
) -> List:
    """
    Get a list of hdfs file paths for a given set of date critera and parent path on hdfs.
    Optionally select the latest only.

    Parameters
    ----------
    path
        hdfs file path to search
    selector
        options for selection type: latest, after(date), before(date)
    start_date
        date to select files after
    end_date
        date to select files before
    """
    file_df = list_contents(path, date_from_filename=True)
    file_df = file_df.dropna(subset=["upload_date"])
    file_df = file_df.sort_values(["upload_date", "upload_time"])

    if start_date is not None:
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
        file_df = file_df[file_df["upload_date"].dt.date >= start_date]
    if end_date is not None:
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y-%m-%d")
        file_df = file_df[file_df["upload_date"].dt.date <= end_date]

    file_list = file_df["file_path"].tolist()
    if latest_only and len(file_list) > 0:
        file_list = [file_list[-1]]
    return file_list


def get_files_not_processed(file_list: list, table_name: str):
    """
    Returns a list of files (to process) which haven't yet been processed
    Parameters
    ----------
    file_list: List of all files for processing
    table_name: Hive table which contains the list of files already processed
    ----------
    """
    spark_session = get_or_create_spark_session()
    storage_config = get_config()["storage"]
    if check_table_exists(table_name):
        df_processed = spark_session.sql(
            f"SELECT DISTINCT processed_filename FROM \
            {storage_config['database']}.{storage_config['table_prefix']}{table_name}"
        )
        processed_file_list = [row[0] for row in df_processed.collect()]

        unprocessed_file_list = [i for i in file_list if i not in processed_file_list]

        return unprocessed_file_list

    else:
        return file_list


def get_files_to_be_processed(
    resource_path,
    latest_only=False,
    start_date=None,
    end_date=None,
    include_processed=False,
    include_invalid=False,
):
    """
    Get list of files matching the specified pattern and optionally filter
    to only those that have not been processed or were previously invalid.
    """
    file_paths = get_files_by_date(resource_path, latest_only, start_date, end_date)
    if not include_processed:
        file_paths = get_files_not_processed(file_paths, "processed_filenames")
    if check_table_exists("error_file_log") and not include_invalid:
        file_error_log_df = extract_from_table(get_config()["table_names"]["input"]["error"])
        error_file_paths = column_to_list(file_error_log_df, "file_path")
        file_paths = [p for p in file_paths if p not in error_file_paths]
    return file_paths
