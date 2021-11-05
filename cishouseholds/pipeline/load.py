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


def add_run_log_entry(config: dict, run_datetime: datetime):
    """
    Adds an entry to the pipeline's run log. Pipeline name is inferred from the Spark App name.
    """
    spark_session = get_or_create_spark_session()
    storage_config = config["storage"]
    pipeline_name = spark_session.sparkContext.appName
    pipeline_version = pkg_resources.get_distribution(pipeline_name).version

    run_id = 0

    if check_table_exists("run_log"):
        last_run_id = get_latest_run_id(storage_config)
        run_id = last_run_id + 1

    run_log_entry = _create_run_log_entry(config, spark_session, run_datetime, run_id, pipeline_version, pipeline_name)
    run_log_entry.write.mode("append").saveAsTable(
        f'{storage_config["database"]}.{storage_config["table_prefix"]}run_log'
    )  # Always append
    return run_id


def get_latest_run_id(storage_config):
    """Read the maximum run ID from the run log table."""
    spark_session = get_or_create_spark_session()
    run_log_table = f'{storage_config["database"]}.{storage_config["table_prefix"]}run_log'
    return spark_session.read.table(run_log_table).select(F.max("run_id")).first()[0]


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


def update_table_and_log_source_files(df: DataFrame, table_name: str, filename_column: str):
    """
    Update a table with the specified dataframe and log the source files that have been processed.
    Used to record which files have been processed for each input file type.
    """
    update_table(df, table_name)
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

    run_id = get_latest_run_id(storage_config)

    entry = [[run_id, file_type, filename, datetime.now()] for filename in newly_processed_files]
    df = spark_session.createDataFrame(entry, schema)
    table_name = f'{storage_config["database"]}.{storage_config["table_prefix"]}processed_filenames'
    df.write.mode("append").saveAsTable(table_name)  # Always append
