import pyspark.sql.functions as F
from chispa import assert_df_equality
from pyspark.sql import DataFrame

from cishouseholds.impute import impute_and_flag


def _example_imputer(df: DataFrame, column_name_to_assign: str, reference_column: str, literal=1):
    """Imputes a column with a constant literal"""
    return df.withColumn(
        column_name_to_assign, F.when(F.col(reference_column).isNull(), F.lit(literal)).otherwise(None)
    )


def test_impute_wrapper_first_run(spark_session):
    """Check imputation flag creation on first run"""

    input_data = [
        ("A", None),
        ("B", None),
        ("C", 1),
    ]

    expected_data = [
        ("A", 1, 1, "example_imputer"),
        ("B", 1, 1, "example_imputer"),
        ("C", 1, 0, None),
    ]

    input_df = spark_session.createDataFrame(
        data=input_data,
        schema="""
            group string,
            value integer
        """,
    )

    expected_df = spark_session.createDataFrame(
        data=expected_data,
        schema="""
            group string,
            value integer,
            value_is_imputed integer,
            value_imputation_method string
        """,
    )

    actual_df = impute_and_flag(input_df, imputation_function=_example_imputer, reference_column="value", literal=1)
    assert_df_equality(actual_df, expected_df, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)


def test_impute_wrapper_subsequent_run(spark_session):
    """Check that existing imputation flags are maintained in subsequent runs"""
    schema = """
            group string,
            value integer,
            value_is_imputed integer,
            value_imputation_method string
        """
    input_data = [
        ("A", 1, 1, 1, "example_imputer"),
        ("B", 1, None, None, None),
    ]

    expected_data = [
        ("A", 1, 1, "example_imputer"),
        ("B", 1, 1, "example_imputer"),
    ]

    input_df = spark_session.createDataFrame(
        data=input_data,
        schema=schema,
    )
    expected_df = spark_session.createDataFrame(
        data=expected_data,
        schema=schema,
    )

    actual_df = impute_and_flag(input_df, imputation_function=_example_imputer, reference_column="value", literal=1)
    assert_df_equality(actual_df, expected_df, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)
