from pyspark.sql.types import StructField

from cishouseholds.pyspark_utils import convert_cerberus_schema_to_pyspark


def test_conversion():
    cerberus_schema = {"id": {"type": "string"}, "whole_number": {"type": "integer"}}

    pyspark_schema = convert_cerberus_schema_to_pyspark(cerberus_schema)

    assert len(pyspark_schema) == len(cerberus_schema)
    assert sorted([column_schema.name for column_schema in pyspark_schema]) == sorted(cerberus_schema.keys())
    assert all(isinstance(column_schema, StructField) for column_schema in pyspark_schema)
