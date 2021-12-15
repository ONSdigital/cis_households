from chispa import assert_df_equality

from cishouseholds.edit import cast_columns_from_string


def test_re_cast_columns_from_string(spark_session):

    schema_input_df = """col_to_cast_1 string,
                         col_to_cast_2 string,
                         col_to_cast_3 string"""
    data_input_df = [
        ("1", "3.0", "5.5"),
        ("2", "4.55", None),
    ]

    schema_expected_df = """col_to_cast_1 double,
                            col_to_cast_2 double,
                            col_to_cast_3 double"""
    data_expected_df = [
        (1.0, 3.0, 5.5),
        (2.0, 4.55, None),
    ]

    input_df = spark_session.createDataFrame(data_input_df, schema=schema_input_df)

    expected_df = spark_session.createDataFrame(data_expected_df, schema=schema_expected_df)

    columns_list = ["col_to_cast_1", "col_to_cast_2", "col_to_cast_3"]

    output_df = cast_columns_from_string(input_df, columns_list, "double")

    assert_df_equality(output_df, expected_df, ignore_row_order=True, ignore_column_order=True)
