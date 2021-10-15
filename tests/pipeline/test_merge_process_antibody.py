# from chispa import assert_df_equality
from cishouseholds.pipeline.merge_process import execute_and_resolve_flags_merge_specific_antibody


def test_merge_process_antibody(spark_session):
    schema = "barcode string, any string"
    data = [
        ("ONS0001", None),
        ("ONS0002", None),
        ("ONS0003", None),
        ("ONS0003", None),
        ("ONS0003", None),
        ("ONS0004", None),
        ("ONS0004", None),
        ("ONS0004", None),
    ]
    df_input_survey = spark_session.createDataFrame(data, schema=schema).drop("any")

    schema = """barcode string,
                date_visit string,
                date_received string,
                antibody_result_recorded_datetime string,
                antibody_test_result_classification string,
                siemens string,
                tdi string"""
    data = [
        ("ONS0001", "2020-01-01", "2020-01-02", "2020-01-04 12:00:00", "positive", "negative", "positive"),
        ("ONS0002", "2020-01-02", "2020-01-03", "2020-01-04 12:00:00", "negative", "positive", "negative"),
        ("ONS0002", "2020-01-02", "2020-01-03", "2020-01-04 12:00:00", "positive", "negative", "positive"),
        ("ONS0002", "2020-01-02", "2020-01-03", "2020-01-04 12:00:00", "negative", "positive", "negative"),
        ("ONS0003", "2020-01-03", "2020-01-04", "2020-01-04 12:00:00", "positive", "negative", "positive"),
        ("ONS0004", "2020-01-04", "2020-01-05", "2020-01-04 12:00:00", "negative", "positive", "negative"),
        ("ONS0004", "2020-01-04", "2020-01-05", "2020-01-04 12:00:00", "positive", "negative", "positive"),
        ("ONS0004", "2020-01-04", "2020-01-05", "2020-01-04 12:00:00", "negative", "positive", "negative"),
    ]
    df_input_antibody = spark_session.createDataFrame(data, schema=schema)

    # add expected dataframe

    execute_and_resolve_flags_merge_specific_antibody(df_input_survey, df_input_antibody, "date_visit")
    # assert_df_equality(output_df)
