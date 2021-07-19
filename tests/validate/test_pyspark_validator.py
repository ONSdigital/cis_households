from datetime import datetime

from cishouseholds.validation import PysparkValidator


def test_validator_with_timestamp(spark_session):
    schema = {"ts": {"type": "timestamp"}}
    validator = PysparkValidator(schema)

    df = spark_session.createDataFrame([(datetime(3000, 1, 1, 1, 1, 1, 1))], "ts timestamp")

    result = validator(df.rdd.collect()[0])
    assert result
    assert len(validator.errors) == 0
