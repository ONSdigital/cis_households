# from chispa import assert_df_equality
from cishouseholds.pipeline.merge_process import execute_and_resolve_flags_merge_specific_swabs


def test_merge_process_swab(spark_session):
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

    schema = "barcode string, date_visit string, date_received string, pcr_result_recorded_datetime string,\
             pcr_result_classification string"
    data = [
        ("ONS0001", "2020-01-01", "2020-01-02", "2020-01-04 12:00:00", "positive"),
        ("ONS0002", "2020-01-02", "2020-01-03", "2020-01-04 12:00:00", "negative"),
        ("ONS0002", "2020-01-02", "2020-01-03", "2020-01-04 12:00:00", "negative"),
        ("ONS0002", "2020-01-02", "2020-01-03", "2020-01-04 12:00:00", "positive"),
        ("ONS0003", "2020-01-03", "2020-01-04", "2020-01-04 12:00:00", "positive"),
        ("ONS0004", "2020-01-04", "2020-01-05", "2020-01-04 12:00:00", "positive"),
        ("ONS0004", "2020-01-04", "2020-01-05", "2020-01-04 12:00:00", "positive"),
        ("ONS0004", "2020-01-04", "2020-01-05", "2020-01-04 12:00:00", "positive"),
    ]
    df_input_labs = spark_session.createDataFrame(data, schema=schema)

    # add expected dataframe

    execute_and_resolve_flags_merge_specific_swabs(df_input_survey, df_input_labs, "date_visit")
    # assert_df_equality(output_df)
