import pyspark.sql.functions as F
from chispa import assert_df_equality

from cishouseholds.merge import merge_survey_tables


def test_merge_survey_tables(spark_session):
    test_hadoop = False
    if test_hadoop:
        output_df = merge_survey_tables(
            tables=[
                """hdfs:////ons/covserolink/hive/stef_transformed_survey_responses_v0_data/
                "part-00000-a8d04ad1-48da-49e8-8bf1-eebdfd18b55f-c000.snappy.parquet""",
                """hdfs:////ons/covserolink/hive/stef_transformed_survey_responses_v1_data/
                part-00000-78d896b0-e6e2-4ada-a234-06990f47903a-c000.snappy.parquet""",
                """hdfs:////ons/covserolink/hive/stef_transformed_survey_responses_v2_data/
                part-00000-c336180e-d3e1-49cf-8e7e-48c5ed545e60-c000.snappy.parquet""",
            ]
        )
        assert output_df.count() == 150 and output_df.select("visit_id").filter(F.col("visit_id").isNull()).count() == 0
    else:
        input_df0 = spark_session.createDataFrame(
            data=[
                ("a1", 1, 2),
                ("a2", 2, 4),
                ("a2", 3, 6),
            ],
            schema="id string, col1 integer, col3 integer",
        )
        input_df1 = spark_session.createDataFrame(
            data=[
                ("b1", 1, 9),
                ("b2", 1, 9),
                ("b3", 1, 9),
            ],
            schema="id string, col1 integer, col2 integer",
        )
        input_df2 = spark_session.createDataFrame(
            data=[
                ("c1", 1, 27, 26),
                ("c2", 2, 26, 25),
                ("b2", 3, 25, 24),
            ],
            schema="id string, col1 integer, col2 integer, col3 integer",
        )
        expected_df = spark_session.createDataFrame(
            data=[
                ("a1", 1, None, 26),
                ("a2", 2, None, 25),
                ("a2", 3, None, 24),
                ("b1", 1, 9, None),
                ("b2", 1, 9, None),
                ("b3", 1, 9, None),
                ("c1", 1, 27, 26),
                ("c2", 2, 26, 25),
                ("b2", 3, 25, 24),
            ],
            schema="id string, col1 integer, col2 integer, col3 integer",
        )
        output_df = merge_survey_tables([input_df0, input_df1, input_df2])
        assert_df_equality(expected_df, output_df)
