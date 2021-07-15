from cerberus import Validator
from chispa import assert_df_equality
from pyspark.sql import Row

from cishouseholds.pyspark_utils import ListAccumulator
from cishouseholds.validate import filter_and_accumulate_validation_errors


def test_filter(spark_session):
    schema = "id string, value integer"
    input_df = spark_session.createDataFrame([("id1", 1), ("idtwo", 1), ("id3", -1)], schema)

    expected_df = spark_session.createDataFrame([("id1", 1)], schema)

    validator = Validator({"id": {"type": "string", "regex": r"id\d"}, "value": {"type": "integer", "min": 0}})
    error_accumulator = spark_session.sparkContext.accumulator(value=[], accum_param=ListAccumulator())
    actual_df = input_df.rdd.filter(
        lambda r: filter_and_accumulate_validation_errors(
            r, accumulator=error_accumulator, cerberus_validator=validator
        )
    ).toDF(schema=schema)
    assert_df_equality(actual_df, expected_df)


def test_error_accumulation(spark_session):
    schema = "id string, value integer"
    input_df = spark_session.createDataFrame([("id1", 1), ("idtwo", 1), ("id3", -1)], schema)

    expected_errors = [
        (Row(id="idtwo", value=1), {"id": ["value does not match regex 'id\\d'"]}),
        (Row(id="id3", value=-1), {"value": ["min value is 0"]}),
    ]

    validator = Validator({"id": {"type": "string", "regex": r"id\d"}, "value": {"type": "integer", "min": 0}})
    error_accumulator = spark_session.sparkContext.accumulator(value=[], accum_param=ListAccumulator())
    input_df.rdd.filter(
        lambda r: filter_and_accumulate_validation_errors(
            r, accumulator=error_accumulator, cerberus_validator=validator
        )
    ).collect()
    assert sorted(error_accumulator.value) == sorted(expected_errors)
