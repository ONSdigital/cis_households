from chispa import assert_df_equality

from cishouseholds.filter import filter_exclude_by_pattern


def test_filtering_by_known_pattern_returns_filtered_dataframe(spark_session):
    data = [
        ("ONS10938123", 1),
        ("ONS10932347", 1),
        ("ONS10931374", 1),
        ("SMOKE[1-9]", 1),
        ("ONS278SMOKE[1-9]dksj", 1),
        ("ONS278SMOKE2dksj", 1),
    ]
    df = spark_session.createDataFrame(data=data, schema="participant_id string, num_doses integer")

    expected_data = [
        ("ONS10938123", 1),
        ("ONS10932347", 1),
        ("ONS10931374", 1),
    ]
    expected_df = spark_session.createDataFrame(data=expected_data, schema="participant_id string, num_doses integer")

    actual_df = filter_exclude_by_pattern(df, "participant_id", r"SMOKE\[1-9\]|SMOKE[1-9]")

    assert_df_equality(actual_df, expected_df, ignore_row_order=True, ignore_column_order=True)
