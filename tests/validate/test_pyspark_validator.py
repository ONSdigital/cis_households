import pyspark.sql.functions as F

from cishouseholds.validate import PySparkValidator


def test_validator_with_timestamp(spark_session):
    schema = {"ts": {"type": "timestamp"}}
    validator = PySparkValidator(schema)

    df = spark_session.createDataFrame(["1966-07-30 15:00:00"], "string").toDF("ts")
    df = df.withColumn("ts", F.from_unixtime(F.unix_timestamp("ts")).cast("timestamp"))

    result = validator(df.rdd.collect()[0].asDict())
    assert result
    assert len(validator.errors) == 0
