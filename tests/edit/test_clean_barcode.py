from chispa import assert_df_equality

from cishouseholds.edit import clean_barcode


def test_clean_barcode(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            ("ons12345678", "ONS02345678", 1),
            ("ONC12345678", "ONC02345678", 1),
            ("ran  12345678", "ONS02345678", 1),
            ("LO11 12345678", "ONS02345678", 1),
            ("ONS00000000", None, 1),
            ("ON!00000000", None, 1),
        ],
        schema="barcode string, clean_barcode string, edited integer",
    )

    output_df = clean_barcode(expected_df.drop("clean_barcode"), "barcode", "edited")
    assert_df_equality(
        output_df,
        expected_df.drop("barcode").withColumnRenamed("clean_barcode", "barcode"),
        ignore_row_order=True,
        ignore_column_order=True,
    )
