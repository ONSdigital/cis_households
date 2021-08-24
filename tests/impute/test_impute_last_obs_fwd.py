from chispa import assert_df_equality

from cishouseholds.impute import impute_last_obs_carried_forward


def test_last_obs_forward(spark_session):
    schema = "participant_id string, age integer, age_imputation integer"
    expected_df = spark_session.createDataFrame(
        data=[
            # the first age that will be needed isnt the last observation
            # hence DHR_00000000001, the age taken is the last, which is 5
            ("DHR_00000000001", 1, None),
            ("DHR_00000000002", 27, None),
            ("DHR_00000000003", 58, None),
            ("DHR_00000000004", 16, None),
            ("DHR_00000000001", None, 5),
            ("DHR_00000000001", 1, None),
            # age of participant changes
            ("DHR_00000000001", 5, None),
            ("DHR_00000000001", None, 5),
            ("DHR_0000000XXXX", 1, None),
            ("DHR_00000000003", None, 58),
            # Patient not found, do nothing
            ("DHR_XXXXXXXXXXX", None, None),
        ],
        schema=schema,
    )
    df_input = expected_df.drop("age_imputation")
    actual_df = impute_last_obs_carried_forward(df_input, "age_imputation", "participant_id", "age")
    assert_df_equality(actual_df, expected_df, ignore_row_order=True, ignore_column_order=True)
