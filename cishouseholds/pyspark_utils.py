from typing import Mapping

from pyspark import AccumulatorParam
from pyspark.sql.types import StructType


class ListAccumulator(AccumulatorParam):
    def zero(self, init_value: list) -> list:
        return init_value

    def addInPlace(self, v1: list, v2: list) -> list:
        return v1 + v2


def convert_cerberus_schema_to_pyspark(schema: Mapping[str, Mapping]) -> StructType:
    """
    Convert a cerberus validation schema to a pyspark schema.

    Assumes that schema is not nested.
    The following are required in spark schema:
    * `nullable` is False by default
    * `metadata` is an empty dict by default
    * `name` is the name of the field
    """
    fields = [{"metadata": {}, "name": name, "nullable": False, **values} for name, values in schema.items()]
    return StructType.fromJson({"fields": fields, "type": "struct"})
