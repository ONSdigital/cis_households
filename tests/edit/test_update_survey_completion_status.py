import pyspark.sql.functions as F
from chispa import assert_df_equality

from cishouseholds.edit import update_survey_completion_status


def test_update_survey_completion_status(spark_session):

    input_df = spark_session.createDataFrame(
        data=[
            ("Complete", None, None, "2022-01-01", "2022-01-01"),
            ("Complete", None, "value", "2022-01-01", "2022-01-01"),
            ("Complete", None, None, "2020-01-01", "2022-01-01"),
        ],
        schema="""col1 string, col2 string, col3 string, end string, file_date string""",
    )

    expected_df = spark_session.createDataFrame(
        data=[
            ("Never completed", None, None, "2020-01-01", "2022-01-01"),
            ("Complete", None, "value", "2022-01-01", "2022-01-01"),
            ("Not yet completed", None, None, "2022-01-01", "2022-01-01"),
        ],
        schema="""col1 string, col2 string, col3 string, end string, file_date string""",
    )

    for df in [input_df, expected_df]:
        for col in ["end", "file_date"]:
            df = df.withColumn(col, F.to_timestamp(col))

    output_df = update_survey_completion_status(input_df, "col1", "end", ["col2"])

    assert_df_equality(output_df, expected_df, ignore_row_order=True, ignore_column_order=True)
