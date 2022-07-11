import pyspark.sql.functions as F
from chispa import assert_df_equality
from pyspark.sql import DataFrame

from cishouseholds.impute import impute_and_flag


def test_impute_wrapper(spark_session):
    expected_data_1 = [
        # group,    value,  imputed_value, value_is_imputed,    value_imputation_method
        ("A", None, 1, 1, "example_imputer"),
        ("B", None, 1, 1, "example_imputer"),
        ("C", 1, 1, 0, None),
        ("D", 1, 1, 0, None),
    ]

    expected_data_2 = [
        # group,    value,  imputed_value, value_is_imputed,    value_imputation_method
        ("A", None, 1, 1, "example_imputer"),
        ("B", None, 1, 1, "example_imputer"),
        ("C", 1, 1, 0, None),
        ("D", 1, 1, 1, None),  # positive is imputed valued should be retained
    ]

    def example_imputer(df: DataFrame, column_name_to_assign: str, reference_column: str, literal=1):
        # imputes value with `literal`
        return df.withColumn(
            column_name_to_assign, F.when(F.col(reference_column).isNull(), F.lit(literal)).otherwise(None)
        )

    schema = """
            group string,
            value integer,
            imputed_value integer,
            value_is_imputed integer,
            value_imputation_method string
        """
    df1 = spark_session.createDataFrame(
        data=expected_data_1,
        schema=schema,
    )
    df2 = spark_session.createDataFrame(
        data=expected_data_2,
        schema=schema,
    )

    df_input1 = df1.drop("imputed_value", "value_imputation_method", "value_is_imputed")
    df_input2 = df2.drop("imputed_value")
    expected_df1 = df1.withColumn("value", F.col("imputed_value")).drop("imputed_value")
    expected_df2 = df2.withColumn("value", F.col("imputed_value")).drop("imputed_value")

    actual_df1 = impute_and_flag(
        df_input1, imputation_function=example_imputer, reference_column="value", literal=1
    )  # test with dropped imputation flags to represent no exisiting table
    assert_df_equality(actual_df1, expected_df1, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)

    actual_df2 = impute_and_flag(
        df_input2, imputation_function=example_imputer, reference_column="value", literal=1
    )  # tesst without dropping flags to ensure positives are retained
    assert_df_equality(actual_df2, expected_df2, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)
