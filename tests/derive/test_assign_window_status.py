from datetime import datetime

from chispa import assert_df_equality

from cishouseholds.derive import assign_window_status


def test_assign_window_status(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            ("2020-01-01", "2020-01-30", "Open"),
            ("2020-02-01", "2020-02-30", "New"),
            ("2019-12-01", "2019-12-30", "Closed"),
        ],
        schema="start string, end string, result string",
    )
    output_df = assign_window_status(
        df=expected_df.drop("result"),
        column_name_to_assign="result",
        window_start_column="start",
        window_end_column="end",
        current_date=datetime.strptime("2020-01-15", "%Y-%m-%d"),
    )
    assert_df_equality(output_df, expected_df, ignore_nullable=True, ignore_row_order=True)
