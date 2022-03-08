from chispa import assert_df_equality

from cishouseholds.impute import impute_by_distribution


# debating whether to fix seed or mock F.rand
def test_impute_distribution(spark_session):
    expected_data = [
        # current stata uses 1, 2 for "male", "female"
        ("E12000007", "white", "male", None),  # proportion white-male = 1.0 therefore all rnd < 1.0
        ("E12000007", "white", "male", None),
        ("E12000007", "white", None, "male"),
        ("E12000007", "other", "female", None),  # proportion other-male = 0.0 therefore all rnd > 0.0
        ("E12000007", "other", "female", None),
        ("E12000007", "other", None, "female"),
    ]

    expected_df = spark_session.createDataFrame(
        data=expected_data, schema="GOR9D string, ethnicity_white string, dvsex string, impute_value string"
    )
    df_input = expected_df.drop("impute_value")

    actual_df = impute_by_distribution(
        df_input,
        column_name_to_assign="impute_value",
        reference_column="dvsex",
        group_by_columns=["GOR9D", "ethnicity_white"],
        first_imputation_value="male",
        second_imputation_value="female",
        # seed not used directly for test at present
        rng_seed=42,
    )

    assert_df_equality(actual_df, expected_df, ignore_row_order=True, ignore_column_order=True)
