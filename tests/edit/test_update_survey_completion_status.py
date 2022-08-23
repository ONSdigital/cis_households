import pyspark.sql.functions as F
from chispa import assert_df_equality

from cishouseholds.edit import update_survey_completion_status


def test_update_survey_completion_status(spark_session):

    expected_df = spark_session.createDataFrame(
        data=[
            ("Never completed", None, None, "2022-01-04", "2022-01-05"),
            ("Not yet completed", "2022-01-01", None, "2022-01-06", "2022-01-05"),
            ("Completed", "2022-01-01", "2022-01-02", "2022-01-06", "2022-01-05"),
        ],
        schema="""result string, started_date string, completed_date string, end_date string, file_date string""",
    )

    input_df = expected_df.drop("result")
    for col in input_df.columns:
        input_df = input_df.withColumn(col, F.to_timestamp(col))
        expected_df = expected_df.withColumn(col, F.to_timestamp(col))

    output_df = update_survey_completion_status(input_df, "result", "end_date", "started_date", "completed_date")

    assert_df_equality(output_df, expected_df, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)
