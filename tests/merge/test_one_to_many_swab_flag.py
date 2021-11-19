import pytest
from chispa import assert_df_equality

from cishouseholds.merge import merge_one_to_many_swab_ordering_logic
from cishouseholds.merge import merge_one_to_many_swab_result_pcr_logic
from cishouseholds.merge import merge_one_to_many_swab_time_difference_logic
from cishouseholds.merge import one_to_many_swabs


def test_merge_one_to_many_swab_ordering_logic(spark_session):
    schema = """barcode_iq string,
                abs_diff_24 integer,
                time_diff integer,
                date_received string,
                flag_time_date integer"""
    data = [
        ("A", 24, 0, "2029-01-01", None),
        ("A", 48, 0, "2029-01-01", 1),
        ("B", 0, 12, "2029-01-01", 1),
        ("B", 0, 0, "2029-01-01", None),
        ("B", 0, 2, "2029-01-01", 1),
        ("C", 1, 1, "2029-01-03", None),
        ("D", 1, 1, "2029-01-04", 1),
        ("D", 1, 1, "2029-01-01", None),
        ("E", 2, 2, "2029-01-01", None),
        ("E", 2, 2, "2029-01-01", None),
    ]

    expected_df = spark_session.createDataFrame(data, schema=schema)

    df_input = expected_df.drop("flag")
    ordering_columns = ["abs_diff_24", "time_diff", "date_received"]
    group_by_column = "barcode_iq"

    df_output = merge_one_to_many_swab_ordering_logic(df_input, group_by_column, ordering_columns, "flag_time_date")

    assert_df_equality(df_output, expected_df, ignore_row_order=True, ignore_column_order=True)


def test_merge_one_to_many_pcr_void(spark_session):
    schema = """barcode_iq string,
                result_pcr string,
                flag_pcr integer"""
    data = [
        ("A", "positive", None),
        ("A", "negative", None),
        ("B", "positive", None),
        ("B", "negative", None),
        ("B", "void", 1),
        ("C", "void", None),  # C has no positive/negative
        ("E", "void", 1),  # E has a void having positive/negative
        ("E", "positive", None),
        ("F", "void", None),  # F has no positive/negative, not to flag
        ("F", "void", None),
    ]

    df_expected = spark_session.createDataFrame(data, schema=schema)
    df_input = df_expected.drop("flag_pcr")
    df_actual = merge_one_to_many_swab_result_pcr_logic(df_input, "void", "barcode_iq", "result_pcr", "flag_pcr")

    assert_df_equality(df_actual, df_expected, ignore_row_order=True, ignore_column_order=True)


def test_merge_one_to_many_swab_time_difference_logic(spark_session):
    schema = """barcode_iq string,
                time_diff integer,
                time_diff_abs integer,
                date_received string,
                flag_time_diff integer"""
    data = [
        ("A", 24, 0, "2029-01-02", None),
        ("A", 0, 24, "2029-01-01", 1),
        ("B", 24, 0, "2029-01-01", None),
        ("B", 24, 0, "2029-01-02", 1),
        ("C", -6, 30, "2029-01-01", None),
        ("C", -12, 36, "2029-01-02", 1),
        ("C", -48, 72, "2029-01-03", 1),
        ("D", 12, 12, "2029-01-02", None),
        ("D", -6, 30, "2029-01-01", 1),
        ("D", 48, 72, "2029-01-03", 1),
        ("E", 48, 72, "2029-01-03", None),
        ("E", 48, 72, "2029-01-03", None),
        ("F", -48, 72, "2029-01-03", None),
        ("F", -48, 72, "2029-01-03", None),
    ]

    df_expected = spark_session.createDataFrame(data, schema=schema)
    df_input = df_expected.drop("flag")

    df_output = merge_one_to_many_swab_time_difference_logic(
        df=df_input,
        group_by_column="barcode_iq",
        ordering_columns=["barcode_iq", "time_diff_abs", "time_diff", "date_received"],
        time_difference_logic_flag_column_name="flag_time_diff",
    )

    assert_df_equality(df_output, df_expected, ignore_row_order=True, ignore_column_order=True)


def test_one_to_many_swab(spark_session):
    schema = """barcode_iq string,
                count_voyager integer,
                count_swab integer,
                date_received string,
                date_diff integer,
                date_abs_diff_24 integer,
                out_of_range integer,
                result_pcr string,
                1tom_swabs_flag integer,
                identify_1tom_swabs_flag integer"""

    data = [
        # fmt: off
        # record A - boolean_pass, chose the earliest day
        ("A", 1, 2, "2029-01-02", 48, 24, None, "positive", 1, 1),  # wont pass as its later than the other A
        ("A", 1, 2, "2029-01-01", 48, 24, None, "negative", None, 1),

        # record B
        ("B", 1, 3, "2029-01-01", 48, 24, None, "positive", 1, 1),  # drop - filtered out as abs(date - 24h) is larger
        ("B", 1, 3, "2029-01-02", 24, 0, None, "negative", None, 1),
        # keep - abs date diff smallest within record even though later day
        ("B", 1, 3, "2029-01-01", 96, 72, 1, "negative", 1, 1),  # drop - not passed because out_of_range

        # record C - flag out as outside of time range
        ("C", 1, 2, "2029-01-01", -48, 72, 1, "negative", None, None),
        # not passed because out_of_range and diff_date negative
        ("C", 1, 2, "2029-01-01", 288, 264, 1, "positive", 1, None),  # not passed because out_of_range

        # record E - one of the result_pcr being Null/void and the other not:
        # not passed because result_pcr different than void available for barcode_iq
        ("E", 1, 2, "2029-01-01", 12, 12, None, "void", 1, 1),  # drop
        ("E", 1, 2, "2029-01-01", 12, 12, None, "positive", None, 1),  # kept

        # record F - both result_pcr being null do not flag
        ("F", 1, 2, "2029-01-01", 12, 12, None, "void", None, 1),  # keep
        ("F", 1, 2, "2029-01-01", 12, 12, None, "void", None, 1),  # keep

        # record G - to be dropped because date_diff have different signs:
        ("G", 1, 2, "2029-01-01", -12, 36, None, "positive", 1, 1),  # drop
        ("G", 1, 2, "2029-01-01", 12, 12, None, "positive", None, 1),
        # keep
        # fmt: on
    ]

    expected_df = spark_session.createDataFrame(data, schema=schema)
    df_input = expected_df.drop("flag_1tom_swabs")

    ordering_columns = ["date_abs_diff_24", "date_diff", "date_received"]

    df_output = one_to_many_swabs(
        df=df_input,
        group_by_column="barcode_iq",
        ordering_columns=ordering_columns,
        pcr_result_column_name="result_pcr",
        void_value="void",
        flag_column_name="1tom_swabs_flag",
    )
    df_output = df_output.drop("time_order_flag", "pcr_flag", "time_difference_flag")
    assert_df_equality(df_output, expected_df, ignore_row_order=True, ignore_column_order=True)
