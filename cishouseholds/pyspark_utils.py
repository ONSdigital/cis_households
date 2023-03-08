import os
from typing import Any
from typing import Dict
from typing import Mapping

import pyspark.sql.functions as F
from pandas.core.frame import DataFrame
from pyspark.context import SparkContext
from pyspark.sql import DataFrame as DF
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from cishouseholds.pipeline.config import get_config

session_options = {
    "test": {
        "spark.executor.memory": "32g",
        "spark.executor.cores": 4,
        "spark.dynamicAllocation.maxExecutors": 8,
        "spark.sql.shuffle.partitions": 256,
        "spark.task.cpus": 1,
    },
    "l": {
        "spark.executor.memory": "32g",
        "spark.executor.cores": 4,
        "spark.dynamicAllocation.maxExecutors": 16,
        "spark.sql.shuffle.partitions": 256,
        "spark.task.cpus": 1,
    },
    "m": {
        "spark.executor.memory": "32g",
        "spark.executor.cores": 4,
        "spark.dynamicAllocation.maxExecutors": 8,
        "spark.sql.shuffle.partitions": 256,
        "spark.task.cpus": 1,
    },
    "s": {
        "spark.executor.memory": "16g",
        "spark.executor.cores": 2,
        "spark.dynamicAllocation.maxExecutors": 4,
        "spark.sql.shuffle.partitions": 128,
        "spark.task.cpus": 1,
    },
    "xs": {
        "spark.executor.memory": "1g",
        "spark.executor.cores": 1,
        "spark.dynamicAllocation.maxExecutors": 4,
        "spark.sql.shuffle.partitions": 128,
        "spark.task.cpus": 1,
    },
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
    normalised_schema = {}
    for k, v in schema.items():
        if "array" in list(v.values())[0]:
            normalised_schema[k] = {"type": "string"}
        else:
            normalised_schema[k] = v
    fields = [
        {"metadata": {}, "name": name, "nullable": True, **values}
        for name, values in normalised_schema.items()
        if isinstance(values, dict)
    ]
    return StructType.fromJson({"fields": fields, "type": "struct"})


def convert_array_strings_to_array(df: DF, schema: Dict[str, Any]):
    """Converts a dataframe containing array columns represented as strings to a arrays given a schema."""
    array_schema = {k: v["type"] for k, v in schema.items() if "array" in list(v.values())[0]}
    for k, v in array_schema.items():
        df = df.withColumn(k, F.from_json(F.col(k), v))
    return df


def get_or_create_spark_session() -> SparkSession:
    """
    Create a spark_session, hiding console progress and enabling HIVE table overwrite.
    Session size is configured via pipeline config.
    """
    config = get_config()
    session_size = config.get("pyspark_session_size", "xs")
    spark_session_options = session_options[session_size]
    spark_session = (
        SparkSession.builder.config("spark.executor.memory", spark_session_options["spark.executor.memory"])
        # .config("spark.jars.packages", "com.crealytics:spark-excel_2.11:0.12.2")
        .config("spark.executor.cores", spark_session_options["spark.executor.cores"])
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.dynamicAllocation.maxExecutors", spark_session_options["spark.dynamicAllocation.maxExecutors"])
        .config("spark.sql.shuffle.partitions", spark_session_options["spark.sql.shuffle.partitions"])
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
        .config("spark.shuffle.service.enabled", "true")
        .config("spark.sql.crossJoin.enabled", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.task.cpus", spark_session_options["spark.task.cpus"])
        .appName("cishouseholds")
        .enableHiveSupport()
        .getOrCreate()
    )

    return spark_session


def column_to_list(df: DataFrame, column_name: str):
    """Fast collection of all records in a column to a standard list."""
    return [row[column_name] for row in df.collect()]


def running_in_dev_test() -> bool:
    """Convenience function to check if the code is executing in DevTest environment.

    We test whether the code is executing in DevTest or not by inspecting the SPARK_HOME
    environment variable - which is usuaully set to
    `/opt/cloudera/parcels/CDH-6.3.x-x.cdh6.x.x.p0.xxxxxxxx/lib/spark`
    """
    expected_prefix_in_devtest_spark_home = "/opt/cloudera/parcels/CDH"

    spark_home = os.getenv("SPARK_HOME", "")

    return spark_home.startswith(expected_prefix_in_devtest_spark_home)
