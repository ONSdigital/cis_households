import sys
from typing import Callable
from typing import List
from typing import Union

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def calculate_imputation_from_distribution(
    df: DataFrame,
    column_name_to_assign: str,
    reference_column: str,
    group_columns: List[str],
    first_imputation_value: Union[str, bool, int, float],
    second_imputation_value: Union[str, bool, int, float],
    rng_seed: int = None,
) -> DataFrame:
    """
    Calculate a imputation value from a missing value using a probability
    threshold determined by the proportion of a sub group.
    Defined only for imputing a binary column.
    Parameters
    ----------
    df
    column_name_to_assign
        The colum that will be created with the impute values
    reference_column
        The column for which imputation values will be calculated
    group_columns
        Grouping columns used to determine the proportion of the reference values
    first_imputation_value
        Imputation value if random number less than proportion
    second_imputation_value
        Imputation value if random number greater than or equal to proportion
    rng_seed
        Random number generator seed for making function deterministic.
    Notes
    -----
    Function provides a column value for each record that needs to be imputed.
    Where the value does not need to be imputed the column value created will be null.
    """
    # .rowsBetween(-sys.maxsize, sys.maxsize) fixes null issues for counting proportions
    window = Window.partitionBy(*group_columns).orderBy(reference_column).rowsBetween(-sys.maxsize, sys.maxsize)

    df = df.withColumn(
        "numerator", F.sum(F.when(F.col(reference_column) == first_imputation_value, 1).otherwise(0)).over(window)
    )

    df = df.withColumn("denominator", F.sum(F.when(F.col(reference_column).isNotNull(), 1).otherwise(0)).over(window))

    df = df.withColumn("proportion", F.col("numerator") / F.col("denominator"))

    df = df.withColumn("random", F.rand(rng_seed))

    df = df.withColumn(
        "individual_impute_value",
        F.when(F.col("proportion") > F.col("random"), first_imputation_value)
        .when(F.col("proportion") <= F.col("random"), second_imputation_value)
        .otherwise(None),
    )

    # Make flag easier (non null values will be flagged)
    df = df.withColumn(
        column_name_to_assign,
        F.when(F.col(reference_column).isNull(), F.col("individual_impute_value")).otherwise(None),
    )

    return df.drop("proportion", "random", "denominator", "numerator", "individual_impute_value")


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

    return df.drop("temporary_imputation_values")


  def impute_last_obs_carried_forward(
    df: DataFrame, column_name_to_assign: str, column_identity: str, reference_column: str
) -> DataFrame:
    """
    Imputate the LAST observation of a given field by given identity column.
    Parameters
    ----------
    df
    column_name_to_assign
        The colum that will be created with the impute values
    column_identity
        The column that may or may not have a Null value that at some point will
        have a value and the last one should be captured
    reference_column
        The column for which imputation values will be calculated
    """
    window = Window.partitionBy(column_identity)

    # same way F.last(ignorenulls=True) is used, it would be possible to use F.first()
    # should this be included as an option?

    # I am assuming that theres just one df where observations and imputations
    # happen at the same time but in case theres a df with observation separately
    # a concat() can be used and then apply the window function along side

    return df.withColumn(
        column_name_to_assign,
        F.when(F.col(reference_column).isNull(), F.last(F.col(reference_column), ignorenulls=True).over(window)),
    )
