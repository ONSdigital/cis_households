from chispa import assert_df_equality

from cishouseholds.impute import calculate_imputation_from_mode


def test_impute_distribution(spark_session):

    expected_df = spark_session.createDataFrame(
        data=[
            # current stata uses 1, 2 for "male", "female"
            ("E12000007", "white", "male", None),  # proportion white-male = 1.0 therefore all rnd < 1.0
            ("E12000007", "white", "male", None),
            ("E12000007", None, None, "white"),
            ("E12000007", "white", None, None),
            ("E12000007", "other", "female", None),  # proportion other-male = 0.0 therefore all rnd > 0.0
            ("E12000007", "other", "female", None),
            ("E12000007", None, None, "white"),
        ],
        schema="GOR9D string, white_group string, dvsex string, impute_value string",
    )

    df_input = expected_df.drop("impute_value")
    actual_df = calculate_imputation_from_mode(df_input, "impute_value", "white_group")

    assert_df_equality(actual_df, expected_df, ignore_row_order=True, ignore_column_order=True)
