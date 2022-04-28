import os
from typing import Any
from typing import Mapping

from pandas.core.frame import DataFrame
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from cishouseholds.pipeline.config import get_config

sessions = {
    "s": (
        SparkSession.builder.config("spark.executor.memory", "1g")
        .config("spark.executor.cores", 1)
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.dynamicAllocation.maxExecutors", 3)
        .config("spark.sql.shuffle.partitions", 12)
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
        .config("spark.shuffle.service.enabled", "true")
        .config("spark.sql.crossJoin.enabled", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .appName("cishouseholds")
        .enableHiveSupport()
        .getOrCreate()
    ),
    "m": (
        SparkSession.builder.config("spark.executor.memory", "6g")
        .config("spark.executor.cores", 3)
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.dynamicAllocation.maxExecutors", 3)
        .config("spark.sql.shuffle.partitions", 18)
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
        .config("spark.shuffle.service.enabled", "true")
        .config("spark.debug.maxToStringFields", 2000)
        .config("spark.sql.crossJoin.enabled", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .appName("cishouseholds")
        .enableHiveSupport()
        .getOrCreate()
    ),
    "l": (
        SparkSession.builder.config("spark.executor.memory", "10g")
        .config("spark.yarn.executor.memoryOverhead", "1g")
        .config("spark.executor.cores", 5)
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.dynamicAllocation.maxExecutors", 5)
        .config("spark.sql.shuffle.partitions", 200)
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
        .config("spark.shuffle.service.enabled", "true")
        .config("spark.sql.crossJoin.enabled", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .appName("cishouseholds")
        .enableHiveSupport()
        .getOrCreate()
    ),
    "xl": (
        SparkSession.builder.config("spark.executor.memory", "20g")
        .config("spark.yarn.executor.memoryOverhead", "3g")
        .config("spark.executor.cores", 5)
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.dynamicAllocation.maxExecutors", 12)
        .config("spark.sql.shuffle.partitions", 240)
        .config("spark.shuffle.service.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
        .config("spark.shuffle.service.enabled", "true")
        .config("spark.sql.crossJoin.enabled", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .appName("cishouseholds")
        .enableHiveSupport()
        .getOrCreate()
    ),
    "xxl": (
        SparkSession.builder.config("spark.executor.memory", "64g")
        .config("spark.yarn.executor.memoryOverhead", "6g")
        .config("spark.executor.cores", 4)
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.dynamicAllocation.maxExecutors", 15)
        .config("spark.sql.shuffle.partitions", 1000)
        .config("spark.shuffle.service.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
        .config("spark.shuffle.service.enabled", "true")
        .config("spark.sql.crossJoin.enabled", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .appName("cishouseholds")
        .enableHiveSupport()
        .getOrCreate()
    ),
}


def get_spark_ui_url():
    "Get the URL to open the Spark UI for the current spark session."
    return f"http://spark-{os.environ['CDSW_ENGINE_ID']}.{os.environ['CDSW_DOMAIN']}"


def get_spark_application_id():
    "Get the spark application ID, for use in debugging applications."
    sc = SparkContext.getOrCreate()
    return sc._jsc.sc().applicationId()


def convert_cerberus_schema_to_pyspark(schema: Mapping[str, Any]) -> StructType:
    """
    Convert a cerberus validation schema to a pyspark schema.

    Assumes that schema is not nested.
    The following are required in spark schema:
    * `nullable` is False by default
    * `metadata` is an empty dict by default
    * `name` is the name of the field
    """
    fields = [
        {"metadata": {}, "name": name, "nullable": True, **values}
        for name, values in schema.items()
        if isinstance(values, dict)
    ]
    return StructType.fromJson({"fields": fields, "type": "struct"})


def get_or_create_spark_session() -> SparkSession:
    """
    Create a spark_session, hiding console progress and enabling HIVE table overwrite.
    Session size is configured via pipeline config.
    """
    config = get_config()
    session_size = config.get("pyspark_session_size", "m")
    spark_session = sessions[session_size]

    return spark_session


def column_to_list(df: DataFrame, column_name: str):
    """Fast collection of all records in a column to a standard list."""
    return [row[column_name] for row in df.collect()]
