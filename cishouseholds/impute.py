import sys
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
    positive_value: Union[str, bool, int, float],
    negative_value: Union[str, bool, int, float],
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
    positive_value
        Imputation value if random number less than proportion
    negative_value
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
        "numerator", F.sum(F.when(F.col(reference_column) == positive_value, 1).otherwise(0)).over(window)
    )

    df = df.withColumn("denominator", F.sum(F.when(F.col(reference_column).isNotNull(), 1).otherwise(0)).over(window))

    df = df.withColumn("proportion", F.col("numerator") / F.col("denominator"))

    df = df.withColumn("random", F.rand(rng_seed))

    df = df.withColumn(
        "individual_impute_value",
        F.when(F.col("proportion") > F.col("random"), positive_value)
        .when(F.col("proportion") <= F.col("random"), negative_value)
        .otherwise(None),
    )

    # Make flag easier (non null values will be flagged)
    df = df.withColumn(
        column_name_to_assign,
        F.when(F.col(reference_column).isNull(), F.col("individual_impute_value")).otherwise(None),
    )

    df = df.drop("proportion", "random", "denominator", "numerator", "individual_impute_value")

    return df
