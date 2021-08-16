import sys
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def impute_from_distribution(
    df: DataFrame,
    column_name_to_assign: str,
    reference_column: str,
    grouping_columns: List[str],
    positive_value,
    negative_value,
    rng_seed: int = None,
) -> DataFrame:

    # I don't know why .rowsBetween(-sys.maxsize, sys.maxsize) fixes null issues in windows but it does
    # as I read on the first SO post, but ignored it for the next hour
    window = Window.partitionBy(*grouping_columns).orderBy(reference_column).rowsBetween(-sys.maxsize, sys.maxsize)

    proportion_column = reference_column + "_proportion"

    # QUESTION: should denominator include those to be imputed?

    df = df.withColumn(
        "numerator", F.sum(F.when(F.col(reference_column) == positive_value, 1).otherwise(0)).over(window)
    )

    df = df.withColumn("denominator", F.sum(F.when(F.col(reference_column).isNotNull(), 1).otherwise(0)).over(window))

    df = df.withColumn(proportion_column, F.col("numerator") / F.col("denominator"))

    df = df.withColumn("random", F.rand(rng_seed))

    df = df.withColumn(
        column_name_to_assign,
        F.when(F.col(proportion_column) > F.col("random"), positive_value)
        .when(F.col(proportion_column) <= F.col("random"), negative_value)
        .otherwise(None),
    )

    df = df.drop(proportion_column, "random", "denominator", "numerator")

    return df
