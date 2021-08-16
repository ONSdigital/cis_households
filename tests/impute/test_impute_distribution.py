from chispa import assert_df_equality

from cishouseholds.impute import impute_from_distribution


# debating whether to fix seed or mock F.rand
def test_impute_distribution(spark_session):
    expected_data = [
        ("E12000007", "white", 1, 1),  # proportion white-1 = 1.0 therefore all rnd < 1.0
        ("E12000007", "white", 1, 1),
        ("E12000007", "white", None, 1),
        ("E12000007", "other", 2, 2),  # proportion other-1 = 0.0 therefore all rnd > 0.0
        ("E12000007", "other", 2, 2),
        ("E12000007", "other", None, 2),
    ]

    expected_df = spark_session.createDataFrame(
        data=expected_data, schema="GOR9D string, white_group string, dvsex integer, impute_value integer"
    )
    df_input = expected_df.drop("impute_value")

    actual_df = impute_from_distribution(
        df_input,
        column_name_to_assign="impute_value",
        reference_column="dvsex",
        grouping_columns=["GOR9D", "white_group"],
        positive_value=1,
        negative_value=2,
        rng_seed=42,
    )

    assert_df_equality(actual_df, expected_df, ignore_row_order=True, ignore_column_order=True)
