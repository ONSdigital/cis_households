from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


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
