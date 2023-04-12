import functools
import json
from datetime import datetime
from typing import List

import pkg_resources
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from cishouseholds.pipeline.config import get_config
from cishouseholds.pyspark_utils import get_or_create_spark_session


class TableNotFoundError(Exception):
    pass


def delete_tables(
    prefix: str = None,
    pattern: str = None,
    table_names: List[str] = [],
    ignore_table_prefix: bool = False,
    protected_tables: List[str] = [],
    drop_protected_tables: bool = False,
):
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
    config = get_config()
    storage_config = config["storage"]
    table_prefix = storage_config["table_prefix"]

    def drop_tables(table_names: List[str]):
        for table_name in table_names:
            if table_name in protected_tables and not drop_protected_tables:
                print(f"{storage_config['database']}.{table_name} will not be dropped as it is protected")  # functional
            else:
                print(f"dropping table: {storage_config['database']}.{table_name}")  # functional
                spark_session.sql(f"DROP TABLE IF EXISTS {storage_config['database']}.{table_name}")
                update_processed_file_log(dataset_name=table_name, currently_processed=False)

    protected_tables = [f"{table_prefix}{table_name}" for table_name in protected_tables]

    if table_names is not None:
        if type(table_names) != list:
            table_names = [table_names]  # type:ignore
        table_names = [f"{table_prefix}{table_name}" for table_name in table_names]
        drop_tables(table_names)
        return

    tables: List[str]

    if prefix is not None:
        if ignore_table_prefix:
            pattern = f"*{prefix}"
        else:
            pattern = f"{table_prefix}_{prefix}"

        tables = (
            spark_session.sql(f"SHOW TABLES IN {storage_config['database']} LIKE '{pattern}*'")
            .select("tableName")
            .toPandas()["tableName"]
            .tolist()
        )
        drop_tables(tables)
        return

    if pattern is not None:
        tables = (
            spark_session.sql(f"SHOW TABLES IN {storage_config['database']} LIKE '*{pattern}*'")
            .select("tableName")
            .toPandas()["tableName"]
            .tolist()
        )
        if not ignore_table_prefix:
            tables = [t for t in tables if t.startswith(table_prefix)]
        drop_tables(tables)


def extract_from_table(
    table_name: str,
    break_lineage: bool = False,
    alternate_prefix: str = None,
    alternate_database: str = None,
    latest_table: bool = False,
) -> DataFrame:
    spark_session = get_or_create_spark_session()
    check_table_exists(
        table_name,
        raise_if_missing=True,
        alternate_prefix=alternate_prefix,
        alternate_database=alternate_database,
        latest_table=latest_table,
    )
    if break_lineage:
        return spark_session.sql(
            f"SELECT * FROM {get_full_table_name(table_name, alternate_prefix, alternate_database, latest_table)}"
        ).checkpoint()
    return spark_session.sql(
        f"SELECT * FROM {get_full_table_name(table_name, alternate_prefix, alternate_database, latest_table)}"
    )


def update_table(
    df: DataFrame,
    table_name,
    write_mode,
    archive=False,
    survey_table=False,
    error_if_cols_differ: bool = True,
    latest_table: bool = False,
):
    from cishouseholds.merge import union_multiple_tables

    if write_mode == "append":
        if check_table_exists(table_name):
            check = extract_from_table(table_name, break_lineage=True, latest_table=latest_table)
            if check.columns != df.columns:
                msg = f"Trying to append to {table_name} but columns differ"  # functional
                if error_if_cols_differ:
                    raise ValueError(msg)
                    return
                else:
                    print(f"    - {msg}")  # functional
                    df = union_multiple_tables([check, df])
                    df = df.distinct()
                    write_mode = "overwrite"
    df.write.mode(write_mode).saveAsTable(get_full_table_name(table_name, latest_table=latest_table))
    add_table_log_entry(table_name, survey_table, write_mode)
    if archive:
        now = datetime.strftime(datetime.now(), "%Y%m%d_%H%M%S")
        df.write.mode(write_mode).saveAsTable(f"{get_full_table_name(table_name)}_{now}")


def check_table_exists(
    table_name: str,
    raise_if_missing: bool = False,
    alternate_prefix: str = None,
    alternate_database: str = None,
    latest_table: bool = False,
):
    spark_session = get_or_create_spark_session()
    full_table_name = get_full_table_name(table_name, alternate_prefix, alternate_database, latest_table)
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
    update_table(file_log_entry, "error_file_log", "append")


def add_table_log_entry(table_name: str, survey_table: bool, write_mode: str):
    """
    Log the state of the updated table to the table log
    """
    run_id = get_run_id()
    file_log_entry = _create_table_log_entry(run_id, table_name, survey_table, write_mode)
    file_log_entry.write.mode("append").saveAsTable(get_full_table_name("table_log"))  # Always append


def add_run_log_entry(run_datetime: datetime):
    """
    Adds an entry to the pipeline's run log. Pipeline name is inferred from the Spark App name.
    """
    spark_session = get_or_create_spark_session()
    pipeline_name = spark_session.sparkContext.appName
    pipeline_version = pkg_resources.get_distribution(pipeline_name).version
    run_id = get_run_id()

    run_log_entry = _create_run_log_entry(run_datetime, run_id, pipeline_version, pipeline_name)
    update_table(run_log_entry, "run_log", "append")
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
        log_table = get_full_table_name(table_short_name="run_log")
        run_id += spark_session.read.table(log_table).select(F.max("run_id")).first()[0]
    return run_id


def get_full_table_name(
    table_short_name, alternate_prefix: str = None, alternate_database: str = None, latest_table: bool = False
):
    """
    Get the full database.table_name address for the specified table.
    Based on database and name prefix from config.
    alternate database offered if want to use alternative to storage settings
    latest table:
       Used for when tables are suffixed with a date e.g. tablename_yyyymmdd. If True will return the latest
       table from the database
    """
    storage_config = get_config()["storage"]
    if alternate_database is not None:
        database = alternate_database
    else:
        database = f'{storage_config["database"]}'
    if alternate_prefix is not None:
        prefix = alternate_prefix
    else:
        prefix = storage_config["table_prefix"]
    if latest_table:
        spark_session = get_or_create_spark_session()
        table_names_df = spark_session.sql(f"show tables in {database} like '{prefix}{table_short_name}_20*'")
        table_names_lst = table_names_df.select("tableName").rdd.flatMap(lambda x: x).collect()
        table_short_name = table_names_lst[-1].replace(f"{prefix}", "")
    return f"{database}.{prefix}{table_short_name}"


def _create_error_file_log_entry(run_id: int, file_path: str, error_text: str):
    """
    Creates an entry (row) to be inserted into the file log
    """
    spark_session = get_or_create_spark_session()
    schema = "run_id integer, run_datetime timestamp, file_path string, error string"

    file_log_entry = [[run_id, datetime.now(), file_path, error_text]]

    return spark_session.createDataFrame(file_log_entry, schema)


def _create_table_log_entry(run_id: int, table_name: str, survey_table: bool, write_mode: str):
    """
    Creates an entry (row) to be inserted into the table log
    """
    spark_session = get_or_create_spark_session()
    schema = "run_id integer, table_name string, survey_table string, write_mode string"

    table_log_entry = [[run_id, table_name, survey_table, write_mode]]

    return spark_session.createDataFrame(table_log_entry, schema)


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


def add_run_status(
    run_id: int,
    run_status: str,
    error_stage: str = None,
    run_error: str = None,
):
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

    df = spark_session.createDataFrame(run_status_entry, schema)
    update_table(df, "run_status", "append")


def update_table_and_log_source_files(
    df: DataFrame,
    table_name: str,
    filename_column: str,
    dataset_name: str,
    override_mode: str = None,
    survey_table: bool = False,
    archive: bool = False,
):
    """
    Update a table with the specified dataframe and log the source files that have been processed.
    Used to record which files have been processed for each input file type.
    """
    update_table(df, table_name, override_mode, survey_table=survey_table, archive=archive)
    create_processed_file_log_entry(df, filename_column, dataset_name, table_name)


def create_processed_file_log_entry(df: DataFrame, filename_column: str, dataset_name: str, table_name: str):
    """Collects a list of unique filenames that have been processed and writes them to the specified table."""
    spark_session = get_or_create_spark_session()
    newly_processed_files = df.select(filename_column).distinct().rdd.flatMap(lambda x: x).collect()
    file_lengths = df.groupBy(filename_column).count().select("count").rdd.flatMap(lambda x: x).collect()
    schema = """
        run_id integer,
        dataset_name string,
        table_name string,
        processed_filename string,
        processed_datetime timestamp,
        file_row_count integer,
        currently_processed boolean
    """
    run_id = get_run_id()
    entry = [
        [run_id, dataset_name, table_name, filename, datetime.now(), row_count, True]
        for filename, row_count in zip(newly_processed_files, file_lengths)
    ]
    df = spark_session.createDataFrame(entry, schema)
    update_table(df, "processed_filenames", "append", error_if_cols_differ=False)


def update_processed_file_log(dataset_name: str, currently_processed: bool):
    """Updates the currently_processed column in the processed file log"""
    df = extract_from_table("processed_filenames", break_lineage=True)
    df = df.withColumn(
        "currently_processed",
        F.when(F.lit(dataset_name).contains(F.col("dataset_name")), currently_processed).otherwise(
            F.col("currently_processed")
        ),
    )
    update_table(df, "processed_filenames", "overwrite", error_if_cols_differ=False)
