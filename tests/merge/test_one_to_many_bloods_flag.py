from chispa import assert_df_equality

from cishouseholds.filter import assign_date_interval_and_flag
from cishouseholds.merge import one_to_many_bloods_flag


def test_one_to_many_bloods_flag(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            (1, "2029-01-01", "ONS00000003", "negative", "ONS00000003", "2029-01-01", 4, "positive", 0.0, None, None),
            (1, "2029-01-01", "ONS00000003", "positive", "ONS00000003", "2029-01-01", 4, "positive", 0.0, 1, None),
            (2, "2029-01-02", "ONS00000004", "negative", "ONS00000004", "2029-01-02", 2, "negative", 0.0, None, None),
            (2, "2029-01-02", "ONS00000004", "negative", "ONS00000004", "2029-01-03", 2, None, 24.0, None, None),
            (1, "2029-01-02", "ONS00000004", "negative", "ONS00000004", "2029-01-02", 2, "negative", 0.0, 1, None),
            (1, "2029-01-01", "ONS00000004", "negative", "ONS00000004", "2029-01-02", 2, "negative", 24.0, 1, None),
            (1, "2029-01-02", "ONS00000004", "negative", "ONS00000004", "2029-01-03", 2, None, 24.0, 1, None),
            (1, "2029-01-01", "ONS00000004", "negative", "ONS00000004", "2029-01-03", 2, None, 48.0, 1, None),
            (1, "2029-01-06", "ONS00000005", "positive", "ONS00000005", "2029-01-05", 3, "positive", -24.0, None, None),
            (1, "2029-01-06", "ONS00000005", "positive", "ONS00000005", "2029-01-06", 3, "positive", 0.0, 1, None),
            (1, "2029-01-04", "ONS00000006", "negative", "ONS00000006", "2029-01-05", 3, "negative", 24.0, None, None),
            (1, "2029-01-04", "ONS00000007", "positive", "ONS00000007", "2029-01-05", 3, "positive", 24.0, None, None),
            (1, "2029-01-04", "ONS00000007", "positive", "ONS00000007", "2029-01-05", 3, "positive", 24.0, None, 1),
        ],
        schema="count_barcode_voyager integer, visit_date string, barcode_iq string, tdi string, barcode_ox string, \
            received_ox_date string, count_barcode_blood integer, siemens string, \
                diff_interval_hours double, one_to_many_bloods_drop_flag integer, failed integer",
    )
    schema_iq = """count_barcode_voyager integer, visit_date string, barcode_iq string, tdi string"""
    data_iq = [
        (2, "2029-01-02", "ONS00000004", "negative"),
        (1, "2029-01-04", "ONS00000006", "negative"),
        (1, "2029-01-01", "ONS00000003", "negative"),
        (1, "2029-01-01", "ONS00000004", "negative"),
        (1, "2029-01-06", "ONS00000005", "positive"),
        (1, "2029-01-04", "ONS00000007", "positive"),
        (1, "2029-01-01", "ONS00000003", "positive"),
        (1, "2029-01-02", "ONS00000004", "negative"),
    ]
    schema_ox = """barcode_ox string, received_ox_date string, count_barcode_blood integer, siemens string"""
    data_ox = [
        ("ONS00000004", "2029-01-03", 2, None),
        ("ONS00000002", "2029-01-04", 2, "positive"),
        ("ONS00000003", "2029-01-01", 4, "positive"),
        ("ONS00000004", "2029-01-02", 2, "negative"),
        ("ONS00000005", "2029-01-05", 3, "positive"),
        ("ONS00000005", "2029-01-06", 3, "positive"),
        ("ONS00000006", "2029-01-05", 3, "negative"),
        ("ONS00000007", "2029-01-05", 3, "positive"),
        ("ONS00000007", "2029-01-05", 3, "positive"),
    ]
    df_iq = spark_session.createDataFrame(data_iq, schema=schema_iq)
    df_ox = spark_session.createDataFrame(data_ox, schema=schema_ox)
    # IQ - 1:m - bloods

    df_mrg = df_iq.join(df_ox, df_iq.barcode_iq == df_ox.barcode_ox, "inner")

    input_df = df_mrg
    print("HERE")

    # assign a column that contains the difference between the visit and recieved dates
    input_df = assign_date_interval_and_flag(
        input_df, "out_of_date_range_blood", "diff_interval", "visit_date", "received_ox_date", -24, 48
    )

    output_df = one_to_many_bloods_flag(input_df, "one_to_many_bloods_drop_flag", "barcode_iq")
    assert_df_equality(output_df, expected_df, ignore_row_order=True)
