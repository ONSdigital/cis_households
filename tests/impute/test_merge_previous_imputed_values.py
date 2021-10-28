from chispa import assert_df_equality

from cishouseholds.impute import merge_previous_imputed_values


def test_merge_previous_imputed_values(spark_session):
    input_data = [
        ("0", 42, "a"),
        ("1", None, "b"),
        ("2", 365, None),
    ]

    lookup_data = [
        ("0", 101, "imputation_method_0", None, None),
        ("1", 500, "imputation_method_1", None, None),
        ("2", None, None, "value", "imputation_method_2"),
    ]

    expected_data = [
        ("0", 42, 0, None, "a", 0, None),
        ("1", 500, 1, "imputation_method_1", "b", 0, None),
        ("2", 365, 0, None, "value", 1, "imputation_method_2"),
    ]

    input_df = spark_session.createDataFrame(
        input_data, schema="id_column string, int_column integer, str_column string"
    )
    lookup_df = spark_session.createDataFrame(
        lookup_data,
        schema="""id_column string, int_column integer, int_column_imputation_method string,
        str_column string, str_column_imputation_method string""",
    )
    expected_df = spark_session.createDataFrame(
        expected_data,
        schema="""id_column string, int_column integer, int_column_is_imputed integer,
        int_column_imputation_method string, str_column string, str_column_is_imputed integer,
        str_column_imputation_method string""",
    )

    actual_df = merge_previous_imputed_values(input_df, lookup_df, "id_column")

    assert_df_equality(actual_df, expected_df, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)
