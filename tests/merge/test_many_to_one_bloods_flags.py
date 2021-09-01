from chispa import assert_df_equality

from cishouseholds.merge import many_to_one_bloods_flag


def test_many_to_one_bloods_flag(spark_session):

    expected_df = spark_session.createDataFrame(
        data=[
            ("ABC123", None, 1, 2, None),
            ("ABC123", 1, 1, 2, None),
            ("ABC456", None, 1, 2, 1),
            ("ABC456", None, 1, 2, 1),
            ("ABC789", 1, 1, 2, None),
            ("ABC789", 1, 1, 2, None),
        ],
        schema="blood_barcode_cleaned string, out_of_date_range_bloods integer, count_barcode_blood integer, \
               count_barcode_v2 integer,drop_many_to_one_bloods_flag integer",
    )

    input_df = expected_df.drop("drop_many_to_one_bloods_flag")

    output_df = many_to_one_bloods_flag(input_df, "drop_many_to_one_bloods_flag")

    assert_df_equality(output_df, expected_df, ignore_row_order=True)
