import functools
import json
import os
from datetime import datetime

import pkg_resources
import pyspark.sql.functions as F
import yaml
from pyspark.sql.session import SparkSession

from cishouseholds.pyspark_utils import get_or_create_spark_session


@functools.lru_cache(maxsize=1)
def get_config() -> dict:
    """Read YAML config file from path specified in PIPELINE_CONFIG_LOCATION environment variable"""
    _config_location = os.environ.get("PIPELINE_CONFIG_LOCATION")
    if _config_location is None:
        raise ValueError("PIPELINE_CONFIG_LOCATION environment variable must be set to the config file path")
    with open(_config_location) as fh:
        config = yaml.load(fh, Loader=yaml.FullLoader)
    return config


def update_table(df, table_name):
    storage_config = get_config()["storage"]
    df.write.mode(storage_config["write_mode"]).saveAsTable(
        f"{storage_config['database']}.{storage_config['table_prefix']}{table_name}"
    )


def extract_from_table(table_name: str, spark_session):
    storage_config = get_config()["storage"]
    df = spark_session.sql(f"SELECT * FROM {storage_config['database']}.{table_name}")

    return df


def add_run_log_entry(config: dict, run_datetime: datetime):
    """
    Adds an entry to the pipeline's run log. Pipeline name is inferred from the Spark App name.
    """
    spark_session = get_or_create_spark_session()
    storage_config = config["storage"]
    pipeline_name = spark_session.sparkContext.appName
    pipeline_version = pkg_resources.get_distribution(pipeline_name).version

    run_log_table = f'{storage_config["database"]}.{storage_config["table_prefix"]}{pipeline_name}_run_log'
    run_id = 0
    if spark_session.catalog._jcatalog.tableExists(run_log_table):
        last_run_id = spark_session.read.table(run_log_table).select(F.max("run_id")).first()[0]
        run_id = last_run_id + 1

    run_log_entry = _create_run_log_entry(config, spark_session, run_datetime, run_id, pipeline_version, pipeline_name)
    run_log_entry.write.mode("append").saveAsTable(run_log_table)  # Always append
    return run_id


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

    run_log_entry = [[run_id, run_datetime, pipeline, version, json.dumps(config)]]

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
    pipeline = spark_session.sparkContext.appName
    run_status_table = f'{storage_config["database"]}.{storage_config["table_prefix"]}{pipeline}_run_status'

    df = spark_session.createDataFrame(run_status_entry, schema)
    df.write.mode("append").saveAsTable(run_status_table)  # Always append
