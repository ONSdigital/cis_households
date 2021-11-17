from chispa import assert_df_equality

from cishouseholds.validate import check_singular_match


def test_check_singular_match(spark_session):
    """Test that validator works to assign failed flags to columns where result is not unique"""
    input_df = spark_session.createDataFrame(  # check that all failed columns pre-specified are overwritten
        data=[
            ("ONS00000001", 1, None),  # check lone records are passed through
            ("ONS00000002", None, None),  # check that records becoming singular after drop are not failed
            ("ONS00000002", 1, 1),
            ("ONS00000003", 1, None),
            ("ONS00000003", 1, None),  # check failed flags cannot apply to dropped columns
            ("ONS00000004", None, 1),
            ("ONS00000004", 1, 1),  # check incorrect failed flags updated
            ("ONS00000005", None, None),
            ("ONS00000005", None, None),  # check incorrect failed flags updated
            ("ONS00000006", None, 1),  # check correct failed flags detected
            ("ONS00000006", None, 1),
            ("ONS00000006", None, 1),
        ],
        schema="barcode string, flag integer, failed integer",
    )
    expected_df = spark_session.createDataFrame(
        data=[
            ("ONS00000001", 1, None),  # check lone records are passed through
            ("ONS00000002", None, None),  # check that records becoming singular after drop are not failed
            ("ONS00000002", 1, None),
            ("ONS00000003", 1, None),
            ("ONS00000003", 1, None),  # check failed flags cannot apply to dropped columns
            ("ONS00000004", None, None),
            ("ONS00000004", 1, None),  # check incorrect true failed flags updated
            ("ONS00000005", None, 1),
            ("ONS00000005", None, 1),  # check incorrect false failed flags updated
            ("ONS00000006", None, 1),  # check correct failed flags detected
            ("ONS00000006", None, 1),
            ("ONS00000006", None, 1),
        ],
        schema="barcode string, flag integer, failed integer",
    )
    output_df = check_singular_match(input_df, "flag", "failed", "barcode")
    assert_df_equality(output_df, expected_df, ignore_row_order=True)
