import functools
import json
from datetime import datetime
from typing import List
from typing import Union

import pkg_resources
import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

from cishouseholds.pipeline.config import get_config
from cishouseholds.pyspark_utils import get_or_create_spark_session


class TableNotFoundError(Exception):
    pass


def delete_tables(prefix: str = None, table_names: Union[str, List[str]] = None, pattern: str = None):
    """
    Deletes HIVE tables. For use at the start of a pipeline run, to reset pipeline logs and data.
    Should not be used in production, as all tables may be deleted.

    Use one or more of the optional parameters.

    Parameters
    ----------
    prefix
        remove all tables with a given table name prefix (see config for current prefix)
    table_names
        one or more absolute table names to delete (including prefix)
    pattern
        drop tables where table name matches pattern in SQL format (e.g. "%_responses_%")
    """
    spark_session = get_or_create_spark_session()
    storage_config = get_config()["storage"]

    if table_names is not None:
        if type(table_names) != list:
            table_names = [table_names]  # type:ignore
        for table_name in table_names:
            print(
                f"dropping table: {storage_config['database']}.{storage_config['table_prefix']}{table_name}"
            )  # functional
            spark_session.sql(
                f"DROP TABLE IF EXISTS {storage_config['database']}.{storage_config['table_prefix']}{table_name}"
            )
    if pattern is not None:
        tables = (
            spark_session.sql(f"SHOW TABLES IN {storage_config['database']} LIKE '{pattern}'")
            .select("tableName")
            .toPandas()["tableName"]
            .tolist()
        )
        for table_name in tables:
            print(f"dropping table: {table_name}")  # functional
            spark_session.sql(f"DROP TABLE IF EXISTS {storage_config['database']}.{table_name}")
    if prefix is not None:
        tables = (
            spark_session.sql(f"SHOW TABLES IN {storage_config['database']} LIKE '{prefix}*'")
            .select("tableName")
            .toPandas()["tableName"]
            .tolist()
        )
        for table_name in tables:
            print(f"dropping table: {table_name}")  # functional
            spark_session.sql(f"DROP TABLE IF EXISTS {storage_config['database']}.{table_name}")


def extract_from_table(table_name: str, break_lineage: bool = False) -> DataFrame:
    spark_session = get_or_create_spark_session()
    check_table_exists(table_name, raise_if_missing=True)
    if break_lineage:
        return spark_session.sql(f"SELECT * FROM {get_full_table_name(table_name)}").checkpoint()
    return spark_session.sql(f"SELECT * FROM {get_full_table_name(table_name)}")


def update_table(df, table_name, write_mode, archive=False):
    df.write.mode(write_mode).saveAsTable(get_full_table_name(table_name))
    if archive:
        now = datetime.strftime(datetime.now(), format="%Y%m%d_%H%M%S")
        df.write.mode(write_mode).saveAsTable(f"{get_full_table_name(table_name)}_{now}")


def check_table_exists(table_name: str, raise_if_missing: bool = False):
    spark_session = get_or_create_spark_session()
    full_table_name = get_full_table_name(table_name)
    table_exists = spark_session.catalog._jcatalog.tableExists(full_table_name)
    if raise_if_missing and not table_exists:
        raise TableNotFoundError(f"Table does not exist: {full_table_name}")
    return table_exists


def add_error_file_log_entry(file_path: str, error_text: str):
    """
    Log the state of the current file to the lookup table
    """
    run_id = get_run_id()
    file_log_entry = _create_error_file_log_entry(run_id, file_path, error_text)
    file_log_entry.write.mode("append").saveAsTable(get_full_table_name("error_file_log"))  # Always append


def add_run_log_entry(run_datetime: datetime):
    """
    Adds an entry to the pipeline's run log. Pipeline name is inferred from the Spark App name.
    """
    spark_session = get_or_create_spark_session()
    pipeline_name = spark_session.sparkContext.appName
    pipeline_version = pkg_resources.get_distribution(pipeline_name).version
    run_id = get_run_id()

    run_log_entry = _create_run_log_entry(run_datetime, run_id, pipeline_version, pipeline_name)
    run_log_entry.write.mode("append").saveAsTable(get_full_table_name("run_log"))  # Always append
    return run_id


@functools.lru_cache(maxsize=1)
def get_run_id():
    """
    Get the current run ID.
    Adds 1 to the latest ID in the ID log and caches this result for this run.
    Returns 1 if the run log table doesn't yet exist.
    """
    run_id = 1
    if check_table_exists("run_log"):
        spark_session = get_or_create_spark_session()
        log_table = get_full_table_name("run_log")
        run_id += spark_session.read.table(log_table).select(F.max("run_id")).first()[0]
    return run_id


def get_full_table_name(table_short_name):
    """
    Get the full database.table_name address for the specified table.
    Based on database and name prefix from config.
    """
    storage_config = get_config()["storage"]
    return f'{storage_config["database"]}.{storage_config["table_prefix"]}{table_short_name}'


def _create_error_file_log_entry(run_id: int, file_path: str, error_text: str):
    """
    Creates an entry (row) to be inserted into the file log
    """
    spark_session = get_or_create_spark_session()
    schema = "run_id integer, run_datetime timestamp, file_path string, error string"

    file_log_entry = [[run_id, datetime.now(), file_path, error_text]]

    return spark_session.createDataFrame(file_log_entry, schema)


def _create_run_log_entry(run_datetime: datetime, run_id: int, version: str, pipeline: str):
    """
    Creates an entry (row) to be inserted into the run log.
    """
    spark_session = get_or_create_spark_session()
    config = get_config()
    schema = """
        run_id integer,
        run_datetime timestamp,
        pipeline_name string,
        pipeline_version string,
        config string
    """

    run_log_entry = [[run_id, run_datetime, pipeline, version, json.dumps(config, default=str)]]

    return spark_session.createDataFrame(run_log_entry, schema)


def add_run_status(run_id: int, run_status: str, error_stage: str = None, run_error: str = None):
    """Append new record to run status table, with run status and any error messages"""
    schema = """
        run_id integer,
        run_status_datetime timestamp,
        run_status string,
        error_stage string,
        run_error string
    """
    run_status_entry = [[run_id, datetime.now(), run_status, error_stage, run_error]]

    spark_session = get_or_create_spark_session()
    run_status_table = get_full_table_name("run_status")

    df = spark_session.createDataFrame(run_status_entry, schema)
    df.write.mode("append").saveAsTable(run_status_table)  # Always append


def update_table_and_log_source_files(
    df: DataFrame,
    table_name: str,
    filename_column: str,
    dataset_name: str,
    override_mode: str = None,
):
    """
    Update a table with the specified dataframe and log the source files that have been processed.
    Used to record which files have been processed for each input file type.
    """
    update_table(df, table_name, override_mode)
    update_processed_file_log(df, filename_column, dataset_name)


def update_processed_file_log(df: DataFrame, filename_column: str, dataset_name: str):
    """Collects a list of unique filenames that have been processed and writes them to the specified table."""
    spark_session = get_or_create_spark_session()
    newly_processed_files = df.select(filename_column).distinct().rdd.flatMap(lambda x: x).collect()
    file_lengths = df.groupBy(filename_column).count().select("count").rdd.flatMap(lambda x: x).collect()
    schema = """
        run_id integer,
        dataset_name string,
        processed_filename string,
        processed_datetime timestamp,
        file_row_count integer
    """
    run_id = get_run_id()
    entry = [
        [run_id, dataset_name, filename, datetime.now(), row_count]
        for filename, row_count in zip(newly_processed_files, file_lengths)
    ]
    df = spark_session.createDataFrame(entry, schema)
    table_name = get_full_table_name("processed_filenames")
    df.write.mode("append").saveAsTable(table_name)  # Always append
