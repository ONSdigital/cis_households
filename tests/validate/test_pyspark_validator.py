# import pyspark.sql.functions as F
# from cishouseholds.validate import PySparkValidator
# def test_validator_with_timestamp(spark_session):
#     """Test that validator works with timestamp and double types, which do not exist in super class"""
#     schema = {"ts": {"type": "timestamp"}, "double": {"type": "double"}}
#     validator = PySparkValidator(schema)
#     df = spark_session.createDataFrame([("1999-07-30 15:00:00", 3.14)], "ts string, double double")
#     df = df.withColumn("ts", F.from_unixtime(F.unix_timestamp("ts")).cast("timestamp"))
#     result = validator(df.rdd.collect()[0].asDict())
#     assert result
#     assert len(validator.errors) == 0
