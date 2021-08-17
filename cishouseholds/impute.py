from typing import Callable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def impute_wrapper(df: DataFrame, imputation_function: Callable, reference_column: str, **kwargs) -> DataFrame:
    """
    Wrapper function for calling imputations, flagging imputed records and recording methods.

    Parameters
    ----------
    df
    imputation_function
        The function that calculates the imputation for a given
        reference column
    reference_column
        The column that will be imputed
    **kwargs
        Key word arguments for imputation_function
    Notes
    -----
    imputation_function is expected to create new column with the values to
    be imputated, and NULL where imputation is not needed.
    """
    df = imputation_function(
        df, column_name_to_assign="temporary_imputation_values", reference_column=reference_column, **kwargs
    )

    is_imputed_name = reference_column + "_is_imputed"

    df = df.withColumn(
        is_imputed_name,
        F.when(F.col("temporary_imputation_values").isNotNull(), 1)
        .when(F.col("temporary_imputation_values").isNull(), 0)
        .otherwise(None),
    )

    df = df.withColumn(
        reference_column + "_imputation_method",
        F.when(F.col(is_imputed_name) == 1, imputation_function.__name__).otherwise(None),
    )

    df = df.withColumn(reference_column, F.coalesce(reference_column, "temporary_imputation_values"))

    df = df.drop("temporary_imputation_values")

    return df
