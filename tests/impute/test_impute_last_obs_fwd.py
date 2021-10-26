from chispa import assert_df_equality

from cishouseholds.impute import impute_by_ordered_fill_forward


def test_last_obs_forward(spark_session):
    schema = "date string, participant_id string, age integer, age_imputation integer"

    # FORWARD OBSERVATION
    expected_df_fwd = spark_session.createDataFrame(
        data=[
            ("2020-01-01", "DHR_00000000001", 1, None),
            ("2020-01-01", "DHR_00000000002", 27, None),
            ("2020-01-01", "DHR_00000000003", 58, None),
            ("2020-01-01", "DHR_00000000004", 16, None),
            ("2020-01-02", "DHR_00000000001", None, 1),
            ("2020-01-03", "DHR_00000000001", 1, None),
            # age of participant changes
            ("2020-01-04", "DHR_00000000001", 5, None),
            # here the updated age should come
            ("2020-01-05", "DHR_00000000001", None, 5),
            # Patient not found
            ("2020-01-06", "DHR_XXXXXXXXXXX", None, None),
            # last observation
            ("2020-01-07", "DHR_00000000005", 45, None),
            ("2020-01-08", "DHR_00000000005", None, 45),
            # information to be carried forward changed
            ("2020-01-09", "DHR_00000000005", 16, None),
            ("2020-01-10", "DHR_00000000005", None, 16),
        ],
        schema=schema,
    )

    df_input = expected_df_fwd.drop("age_imputation")
    actual_df = impute_by_ordered_fill_forward(
        df_input, "age_imputation", "participant_id", "age", "date", order_type="asc"
    )
    assert_df_equality(actual_df, expected_df_fwd, ignore_row_order=True, ignore_column_order=True)

    # BACKWARDS OBSERVATION
    expected_df_bkw = spark_session.createDataFrame(
        data=[
            ("2020-01-02", "DHR_00000000001", None, 2),
            ("2020-01-03", "DHR_00000000001", 2, None),
            ("2020-01-04", "DHR_00000000001", None, 1),
            ("2020-01-05", "DHR_00000000001", 1, None),
        ],
        schema=schema,
    )

    df_input = expected_df_bkw.drop("age_imputation")
    actual_df = impute_by_ordered_fill_forward(
        df_input, "age_imputation", "participant_id", "age", "date", order_type="desc"
    )
    assert_df_equality(actual_df, expected_df_bkw, ignore_row_order=True, ignore_column_order=True)
