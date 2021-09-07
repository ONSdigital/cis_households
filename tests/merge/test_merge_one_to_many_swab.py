from chispa import assert_df_equality
from cis_households import merge_one_to_many_swab_result_mk_logic
from cis_households import search_void_in_list


def test_merge_one_to_many_mk_void(spark_session):

    schema = "barcode_iq string, result_mk string, flag_mk integer"

    data = [
        ("A", "positive", 1),
        ("A", "negative", 1),
        ("B", "positive", 1),
        ("B", "negative", 1),
        ("B", "void", None),
        ("C", "void", 1),  # C has no positive/negative
        ("E", "void", None),  # E has a void having positive/negative
        ("E", "positive", 1),
        ("F", "void", 1),  # F has no positive/negative, not to flag
        ("F", "void", 1),
    ]

    df_expected = spark_session.createDataFrame(data, schema=schema)
    df_input = df_expected.drop("flag_mk")
    df_actual = merge_one_to_many_swab_result_mk_logic(df_input, "barcode_iq", "result_mk", "flag_mk")

    assert_df_equality(df_actual, df_expected, ignore_row_order=True, ignore_column_order=True)


def test_search_void_in_list():
    list1 = ["positive", "negative", "void"]
    list2 = ["positive", "negative"]
    list3 = ["void"]

    assert search_void_in_list(list1) == 1
    assert search_void_in_list(list2) is None
    assert search_void_in_list(list3) is None
