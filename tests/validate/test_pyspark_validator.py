from datetime import datetime

import pyspark.sql.functions as F

from cishouseholds.validate import PySparkValidator


def test_validator_with_timestamp(spark_session):
    schema = {"ts": {"type": "timestamp"}}
    validator = PySparkValidator(schema)

    df = spark_session.createDataFrame(
        [(datetime(3000, 1, 1, 1, 1, 1, 1).strftime("%Y-%m-%d %H:%M:%S"))], schema="ts string"
    )
    df = df.withColumn("ts", F.unix_timestamp(F.col("ts"), "yyyy-MM-dd HH:mm:ss")).cast("timestamp")

    result = validator(df.rdd.collect()[0])
    assert result
    assert len(validator.errors) == 0
