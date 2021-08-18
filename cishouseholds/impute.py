from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def calculate_imputation_from_mode(df: DataFrame, column_name_to_assign: str, reference_column: str) -> DataFrame:
    """
    Get imputation value from given column by most repeated value

    Parameters
    ----------
    df
    column_name_to_assign
        The colum that will be created with the impute values
    reference_column
        The column for which imputation values will be calculated

    Notes
    -----
    Function provides a column value for each record that needs to be imputed.
    Where the value does not need to be imputed the column value created will be null.
    """
    # groupby count() after get all not nulls
    df_notnulls = df.filter(F.col(reference_column).isNotNull()).groupby(df.white_group).count()

    # dump maximum value
    mode_imput = df_notnulls.agg({reference_column: "max"}).collect()[0][0]

    return df.withColumn(column_name_to_assign, F.when(F.col(reference_column).isNull(), mode_imput))
