import pyspark.sql.functions as F

from cishouseholds.merge import merge_survey_tables


def test_merge_survey_tables():
    output_df = merge_survey_tables(
        tables=[
            "hdfs:////ons/covserolink/hive/stef_transformed_survey_responses_v0_data/part-00000-a8d04ad1-48da-49e8-8bf1-eebdfd18b55f-c000.snappy.parquet",
            "hdfs:////ons/covserolink/hive/stef_transformed_survey_responses_v1_data/part-00000-78d896b0-e6e2-4ada-a234-06990f47903a-c000.snappy.parquet",
            "hdfs:////ons/covserolink/hive/stef_transformed_survey_responses_v2_data//part-00000-c336180e-d3e1-49cf-8e7e-48c5ed545e60-c000.snappy.parquet",
        ]
    )

    assert output_df.count() == 150 and output_df.select("visit_id").filter(F.col("visit_id").isNull()).count() == 0
