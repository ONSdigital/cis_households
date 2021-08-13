from typing import Any
from typing import Mapping

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


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
    """
    # To enable overwrite. See https://docs.microsoft.com/en-us/azure/databricks/kb/jobs/spark-overwrite-cancel
    spark_session = (
        SparkSession.builder.config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
        .getOrCreate()
    )

    return spark_session
