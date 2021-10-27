from chispa import assert_df_equality

from cishouseholds.impute import merge_previous_imputed_values


def test_merge_previous_imputed_values(spark_session):
    schema = "id integer, column integer, column_is_imputed integer, column_imputation_method string"

    input_data = [(0, 42, 1, "imputation_method_1"), (1, None, 0, None)]

    lookup_data = [(0, 101, 1, "inputation_method_2"), (1, 500, 1, "imputation_method_3")]

    expected_data = [(0, 42, 1, "imputation_method_1"), (1, 500, 1, "imputation_method_3")]

    input_df = spark_session.createDataFrame(input_data, schema=schema)
    lookup_df = spark_session.createDataFrame(lookup_data, schema=schema)
    expected_df = spark_session.createDataFrame(expected_data, schema=schema)

    actual_df = merge_previous_imputed_values(input_df, lookup_df, "id")

    assert_df_equality(actual_df, expected_df, ignore_row_order=True, ignore_column_order=True)
