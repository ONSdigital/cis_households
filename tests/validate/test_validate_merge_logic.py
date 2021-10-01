from chispa import assert_df_equality

from cishouseholds.validate import validate_merge_logic


def test_validate_merge_logic(spark_session):
    """Test that validator works to assign failed flags to columns where result is not unique"""
    df = spark_session.createDataFrame(
        data=[
            ("ONS00000002", 1, 1, None, None, None, None),
            ("ONS00000003", None, None, None, 1, None, 1),
            ("ONS00000003", None, None, None, 1, None, 1),
            ("ONS00000004", None, None, None, 1, 1, 1),
            ("ONS00000004", None, None, None, 1, 1, 1),  # check failed flags cannot apply to dropped columns
            ("ONS00000005", 1, None, 1, None, None, None),
            ("ONS00000005", 1, None, 1, None, None, None),
            ("ONS00000005", 1, None, 1, None, None, None),  # check correct failed flags detected
            ("ONS00000006", None, None, None, None, None, None),
            ("ONS00000006", None, None, None, None, None, None),  # check a match has been applied
            ("ONS00000007", 1, None, 1, 1, None, 1),  # check cannot flag as more than 1 type
        ],
        schema="barcode string, 1tomb integer, flag1 integer, failed1 integer, mto1s integer,\
             flag2 integer, failed2 integer",
    )

    expected = spark_session.createDataFrame(
        data=[
            ("ONS00000002", 1, 1, None, None, None, None),
            ("ONS00000003", None, None, None, 1, None, 1),
            ("ONS00000003", None, None, None, 1, None, 1),
            ("ONS00000004", None, None, None, 1, 1, None),
            ("ONS00000004", None, None, None, 1, 1, None),  # check failed flags cannot apply to dropped columns
            ("ONS00000005", 1, None, 1, None, None, None),
            ("ONS00000005", 1, None, 1, None, None, None),
            ("ONS00000005", 1, None, 1, None, None, None),  # check correct failed flags detected
            ("ONS00000006", None, None, None, None, None, None),
            ("ONS00000006", None, None, None, None, None, None),  # check a match has been applied
            ("ONS00000007", 1, None, None, 1, None, None),  # check cannot flag as more than 1 type
        ],
        schema="barcode string, 1tomb integer, flag1 integer, failed1 integer, mto1s integer,\
             flag2 integer, failed2 integer",
    )
    result = validate_merge_logic(df, ["flag1", "flag2"], ["failed1", "failed2"], ["1tomb", "mto1s"])
    assert_df_equality(result, expected, ignore_row_order=False)
