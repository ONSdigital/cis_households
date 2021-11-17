import json
from datetime import datetime

import pkg_resources
import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession

from cishouseholds.pipeline.config import get_config
from cishouseholds.pyspark_utils import get_or_create_spark_session


def update_table(df, table_name, mode_overide=None):
    storage_config = get_config()["storage"]
    df.write.mode(mode_overide or storage_config["write_mode"]).saveAsTable(
        f"{storage_config['database']}.{storage_config['table_prefix']}{table_name}"
    )


def check_table_exists(table_name: str):
    storage_config = get_config()["storage"]
    spark_session = get_or_create_spark_session()
    return spark_session.catalog._jcatalog.tableExists(
        f"{storage_config['database']}.{storage_config['table_prefix']}{table_name}"
    )


def extract_from_table(table_name: str):
    spark_session = get_or_create_spark_session()
    storage_config = get_config()["storage"]
    df = spark_session.sql(f"SELECT * FROM {storage_config['database']}.{storage_config['table_prefix']}{table_name}")

    return df


def add_error_file_log_entry(config: dict, file_path: str, run_datetime: datetime):
    """
    Log the state of the current file to the lookup table
    """
    storage_config = config["storage"]
    file_id = get_latest_id(storage_config, "error_file_log", "file_id")
    spark_session = get_or_create_spark_session()
    file_log_entry = _create_file_log_entry(spark_session, file_id, file_path, run_datetime)
    file_log_entry.write.mode("append").saveAsTable(
        f'{storage_config["database"]}.{storage_config["table_prefix"]}run_log'
    )  # Always append
    return file_id


def add_run_log_entry(config: dict, run_datetime: datetime):
    """
    Adds an entry to the pipeline's run log. Pipeline name is inferred from the Spark App name.
    """
    spark_session = get_or_create_spark_session()
    pipeline_name = spark_session.sparkContext.appName
    pipeline_version = pkg_resources.get_distribution(pipeline_name).version
    storage_config = config["storage"]
    run_id = get_latest_id(storage_config, "run_log", "run_id")

    run_log_entry = _create_run_log_entry(config, spark_session, run_datetime, run_id, pipeline_version, pipeline_name)
    run_log_entry.write.mode("append").saveAsTable(
        f'{storage_config["database"]}.{storage_config["table_prefix"]}run_log'
    )  # Always append
    return run_id


def get_latest_id(storage_config, table, id_column):
    id = 0
    if check_table_exists(table):
        last_id = get_latest_id(storage_config, table, id_column)
        id = last_id + 1
        spark_session = get_or_create_spark_session()
        log_table = f'{storage_config["database"]}.{storage_config["table_prefix"]}{table}'
        return spark_session.read.table(log_table).select(F.max(id_column)).first()[0]
    return id


def _create_file_log_entry(spark_session: SparkSession, file_id: int, file_path: str, run_datetime: datetime):
    """
    Creates an entry (row) to be insterted into the file log
    """
    schema = "file_id integer, run_datetime string, file_path string"

    file_log_entry = [[file_id, run_datetime, file_path]]

    return spark_session.createDataFrame(file_log_entry, schema)


def _create_run_log_entry(
    config: dict, spark_session: SparkSession, run_datetime: datetime, run_id: int, version: str, pipeline: str
):
    """
    Creates an entry (row) to be inserted into the run log.
    """
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
    storage_config = get_config()["storage"]
    run_status_table = f'{storage_config["database"]}.{storage_config["table_prefix"]}run_status'

    df = spark_session.createDataFrame(run_status_entry, schema)
    df.write.mode("append").saveAsTable(run_status_table)  # Always append


def update_table_and_log_source_files(df: DataFrame, table_name: str, filename_column: str, override_mode: str = None):
    """
    Update a table with the specified dataframe and log the source files that have been processed.
    Used to record which files have been processed for each input file type.
    """
    update_table(df, table_name, override_mode)
    update_processed_file_log(df, filename_column, table_name)


def update_processed_file_log(df: DataFrame, filename_column: str, file_type: str):
    """Collects a list of unique filenames that have been processed and writes them to the specified table."""
    spark_session = get_or_create_spark_session()
    newly_processed_files = df.select(filename_column).distinct().rdd.flatMap(lambda x: x).collect()
    schema = """
        run_id integer,
        file_type string,
        processed_filename string,
        processed_datetime timestamp
    """
    storage_config = get_config()["storage"]

    run_id = get_latest_id(storage_config, "run_log", "run_id")

    entry = [[run_id, file_type, filename, datetime.now()] for filename in newly_processed_files]
    df = spark_session.createDataFrame(entry, schema)
    table_name = f'{storage_config["database"]}.{storage_config["table_prefix"]}processed_filenames'
    df.write.mode("append").saveAsTable(table_name)  # Always append
