from chispa import assert_df_equality

from cishouseholds.impute import calculate_imputation_from_mode
from cishouseholds.impute import most_common_unique_item


def test_impute_mode(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            # if in same hh majority are white/other
            ("000000000001", "white", None),
            ("000000000001", None, "white"),
            ("000000000007", "white", None),
            ("000000000007", "white", None),
            ("000000000007", "white", None),
            ("000000000007", "other", None),
            ("000000000007", None, "white"),
            ("222222222222", "other", None),
            ("222222222222", "other", None),
            ("222222222222", None, "other"),
            # there's a tie on the ehtnicity for a specific hh
            # all of these 3 rows need to be dropped.
            ("999999999999", "white", None),
            ("999999999999", "other", None),
            ("999999999999", None, None),
            # example where nothing should happen
            ("XXXXXXXXXXXX", "other", None),
        ],
        schema="uac_household string, ethnic string, impute_value string",
    )
    df_input = expected_df.drop("impute_value")
    actual_df = calculate_imputation_from_mode(df_input, "impute_value", "ethnic", "uac_household")
    assert_df_equality(actual_df, expected_df, ignore_row_order=True, ignore_column_order=True)


# Imputation function logic
def test_mode_logic():
    assert most_common_unique_item(["a", "a", "b"]) == "a"  # pass
    assert most_common_unique_item(["a", "a", "b", "b", "c"]) is None  # tie, RETURN: None
    assert most_common_unique_item(["white"]) == "white"  # pass
    assert most_common_unique_item(["white", "other"]) is None  # tie, RETURN: None
    assert most_common_unique_item(["white", "white", "other"]) == "white"  # pass
    assert most_common_unique_item(["other", "other"]) == "other"  # pass
