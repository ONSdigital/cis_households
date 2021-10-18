from chispa import assert_df_equality

from cishouseholds.edit import re_cast_column_if_null
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
    schema = """
                barcode string, unique_id_voyager integer,
                count_barcode_voyager integer,
                date_visit string, date_received string,
                antibody_result_recorded_datetime string,
                antibody_test_result_classification string,
                siemens string, tdi string,
                unique_id_antibody integer,
                count_barcode_antibody integer,
                diff_vs_visit_hr double,
                out_of_date_range_antibody integer,
                abs_offset_diff_vs_visit_hr double,
                identify_one_to_many_antibody_flag integer,
                drop_flag_one_to_many_antibody integer,
                failed_due_to_indistinct_match integer,
                identify_many_to_one_antibody_flag integer,
                drop_flag_many_to_one_antibody integer,
                identify_many_to_many_flag integer,
                failed_flag_many_to_many_antibody integer,
                drop_flag_many_to_many_antibody integer,
                1_to_ma integer, m_to_1a integer, m_to_ma integer,
                failed_one_to_many_antibody integer,
                failed_many_to_one_antibody integer,
                failed_many_to_many_antibody integer
            """
    data = [
        (
            "ONS0002",
            2,
            1,
            "2020-01-02",
            "2020-01-03",
            "2020-01-04 12:00:00",
            "negative",
            "positive",
            "negative",
            2,
            3,
            24.0,
            None,
            0.0,
            1,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            1,
            None,
            None,
            1,
            None,
        ),
        (
            "ONS0002",
            2,
            1,
            "2020-01-02",
            "2020-01-03",
            "2020-01-04 12:00:00",
            "positive",
            "negative",
            "positive",
            3,
            3,
            24.0,
            None,
            0.0,
            1,
            1,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            1,
            None,
            None,
            1,
            None,
        ),
        (
            "ONS0002",
            2,
            1,
            "2020-01-02",
            "2020-01-03",
            "2020-01-04 12:00:00",
            "negative",
            "positive",
            "negative",
            4,
            3,
            24.0,
            None,
            0.0,
            1,
            1,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            1,
            None,
            None,
            1,
            None,
        ),
    ]
    expected_df = spark_session.createDataFrame(data, schema=schema)

    output_df = execute_and_resolve_flags_merge_specific_antibody(
        survey_df=df_input_survey,
        labs_df=df_input_antibody,
        barcode_column_name="barcode",
        visit_date_column_name="date_visit",
        received_date_column_name="date_received",
    )

    # in case a column's schema gets converted to a NullType
    output_df = re_cast_column_if_null(output_df, desired_column_type="integer")

    assert_df_equality(expected_df, output_df)
