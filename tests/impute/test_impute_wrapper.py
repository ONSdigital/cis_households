import pyspark.sql.functions as F
from chispa import assert_df_equality
from pyspark.sql import DataFrame

from cishouseholds.impute import impute_wrapper


def test_impute_wrapper(spark_session):  # test funtion
    expected_data = [("A", None, 1, 1, "example_imputer"), ("B", None, 1, 1, "example_imputer"), ("C", 1, 1, 0, None)]

    def example_imputer(df: DataFrame, column_name_to_assign: str, reference_column: str, literal=1):
        # imputes value with `literal`
        df = df.withColumn(
            column_name_to_assign, F.when(F.col(reference_column).isNull(), F.lit(literal)).otherwise(None)
        )

        return df

    df = spark_session.createDataFrame(
        data=expected_data,
        schema="group string, value integer, \
                imputed_value integer, \
                value_is_imputed integer, value_imputation_method string",
    )

    df_input = df.drop("imputed_value", "value_is_imputed", "value_imputation_method")

    expected_df = df.withColumn("value", F.col("imputed_value")).drop("imputed_value")

    actual_df = impute_wrapper(df_input, imputation_function=example_imputer, reference_column="value", literal=1)

    assert_df_equality(actual_df, expected_df, ignore_row_order=True, ignore_column_order=True)
