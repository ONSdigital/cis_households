from chispa import assert_df_equality

from cishouseholds.impute import calculate_imputation_from_mode


def test_impute_mode(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            ("000000000001", "white", None),  # Case where one white, imputation with white in the other record
            ("000000000001", None, "white"),
            ("000000000007", "white", None),  # Case where there are white/other ethnicities
            ("000000000007", "white", None),  # but white is the most common
            ("000000000007", "white", None),
            ("000000000007", "other", None),
            ("000000000007", None, "white"),
            ("222222222222", "other", None),  # Case where the majority of ethnicity is other,
            ("222222222222", "other", None),  # imputate to other
            ("222222222222", None, "other"),
            ("999999999999", "white", None),  # Case where theres a tie on ethnicities,
            ("999999999999", "other", None),  # no imputation should happen at all
            ("999999999999", None, None),
            ("XXXXXXXXXXXX", "other", None),  # example where nothing should happen
            (
                "000000000007",
                "other",
                None,
            ),  # REFACTORING 970: In case Null/blank most common, should not impute null/blank
            ("000000000007", None, "other"),
            ("000000000007", None, "other"),
            ("000000000008", "white", None),  # REFACTORING 970: Part 2 - for white
            ("000000000008", None, "white"),
            ("000000000008", None, "white"),
        ],
        schema="uac_household string, ethnic string, impute_value string",
    )
    df_input = expected_df.drop("impute_value")
    actual_df = calculate_imputation_from_mode(df_input, "impute_value", "ethnic", "uac_household")
    assert_df_equality(actual_df, expected_df, ignore_row_order=True, ignore_column_order=True)
