from chispa import assert_df_equality

from cishouseholds.filter import give_date_interval_and_flag_if_outside


def test_give_date_interval_and_flag_if_outside(spark_session):
    expected_schema = """
    date_1 string, date_2 string, diff_interval_days double,
    diff_interval_hours double, outside_interval_flag integer
    """
    expected_data = [
        # difference to be out of range - up
        ("2020-01-01 12:00:00", "2020-01-04 12:00:00", 3.0, 72.0, 1),
        # difference to be out of range - down
        ("2020-01-01 12:00:00", "2019-12-20 12:00:00", -12.0, -288.0, 1),
        # difference within range - down
        ("2020-01-01 12:00:00", "2020-01-01 06:00:00", -0.25, -6.0, None),
        # difference within range - up
        ("2020-01-01 12:00:00", "2020-01-01 18:00:00", 0.25, 6.0, None),
        # missing date 2 - nullable
        ("2020-01-01 12:00:00", None, None, None, None),
    ]

    expected_df = spark_session.createDataFrame(expected_data, schema=expected_schema)

    expected_df_h = expected_df.drop("diff_interval_days")
    expected_df_d = expected_df.drop("diff_interval_hours")
    input_df = expected_df.drop("diff_interval_days", "diff_interval_hours", "outside_interval_flag")

    # GIVEN UPPER/LOWER INTERVALS IN HOURS (STANDARD)
    actual_df = give_date_interval_and_flag_if_outside(
        input_df, "outside_interval_flag", "date_1", "date_2", -12, 48, "diff_interval"
    )
    assert_df_equality(actual_df, expected_df_h, ignore_row_order=True, ignore_column_order=True)

    # GIVEN UPPER/LOWER INTERVALS IN DAYS
    actual_df = give_date_interval_and_flag_if_outside(
        input_df, "outside_interval_flag", "date_1", "date_2", -0.5, 2, "diff_interval", interval_format="days"
    )
    assert_df_equality(actual_df, expected_df_d, ignore_row_order=True, ignore_column_order=True)
