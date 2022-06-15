from chispa import assert_df_equality

from cishouseholds.edit import update_column_in_time_window


def test_update_column_in_time_window(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            ("old", "2020-09-09 12:00:00"),
            ("old", "2020-12-09 11:59:59"),
            ("old", "2020-12-09 12:00:00"),
            ("old", "2020-12-09 12:01:00"),
        ],
        schema="col string, date string",
    )
    expected_df = spark_session.createDataFrame(
        data=[
            ("new", "2020-09-09 12:00:00"),
            ("new", "2020-12-09 11:59:59"),
            ("old", "2020-12-09 12:00:00"),
            ("old", "2020-12-09 12:01:00"),
        ],
        schema="col string, date string",
    )
    output_df = update_column_in_time_window(
        input_df, "col", "date", "new", ["2020-01-09T12:00:00", "2020-12-09T21:30:00"]
    )
    assert_df_equality(
        output_df,
        expected_df,
        ignore_row_order=True,
        ignore_column_order=True,
    )
