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
) -> DataFrame:

    window = Window.partitionBy(*grouping_columns)

    proportion_column = reference_column + "_proportion"

    df = df.withColumn(
        proportion_column,
        F.count(F.where(F.col(reference_column) == positive_value)).over(window) / F.size().over(window),
    )

    df = df.withColumn("random", F.rand())

    df = df.withColum(
        column_name_to_assign,
        F.when(F.col(proportion_column) < F.col("random"), positive_value).otherwise(negative_value),
    )

    return df
