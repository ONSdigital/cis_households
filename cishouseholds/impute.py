from collections import Counter
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def calculate_imputation_from_mode(
    df: DataFrame, column_name_to_assign: str, reference_column: str, group_column: str
) -> DataFrame:
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
    # get rid of uac with no nulls
    df_notnulls = df.filter(F.col(reference_column).isNull()).select(F.col(group_column))

    # naming convention
    list_reference_column = "list_" + reference_column

    # window function to collect_list() of all the reference_column items per HH
    window = Window.partitionBy(group_column)
    df_window = (
        df.withColumn(list_reference_column, F.collect_list(F.col(reference_column)).over(window))
        .filter(F.col(reference_column).isNotNull())
        .dropDuplicates([group_column, list_reference_column])
        .drop(reference_column)
    )

    # JOIN: inner join with the previous df with no nulls filtered
    df_notnulls = df_notnulls.join(df_window, [group_column])

    # UDF - User defined function to workout the logic for imputation
    udf_most_common_unique = F.udf(lambda x: most_common_unique_item(x))

    # UDF implementation: with most_common_unique_item() inputation per row is performed
    df_imputed = df_notnulls.withColumn(
        column_name_to_assign, udf_most_common_unique(F.col(list_reference_column))
    ).drop(list_reference_column)

    # JOIN: from initial input df join it with the imputed values for the filtered group_column df
    # filtered group_column: has None columns, no same number of elements to be imputed
    actual_df = df.join(df_imputed, [group_column], "outer")

    # Only impute when the cell of the imputed column is null
    actual_df = actual_df.withColumn(
        column_name_to_assign,
        F.when(
            ((F.col(reference_column) == F.col(column_name_to_assign)) | F.col(reference_column).isNotNull()), None
        ).otherwise(F.col(column_name_to_assign)),
    )
    return actual_df


def most_common_unique_item(list_most_common_unrepeated: List[str]):
    """
    Works out the most common value in a list ignoring if it is repeated

    Parameters
    ----------
    list_most_common_unrepeated
        List of strings that will be ignored if repeated same amount of times
        And worked out which is the most common occurence in case it is unique
    Notes
    -----
    This function is implemented as a User Defined Function in Pyspark to carry
        the imputation for every row.
    """
    count = Counter(list_most_common_unrepeated).most_common(2)
    if len(count) > 1:
        if count[0][1] - count[1][1] == 0:  # if they are the same, there's a tie
            return None
    return Counter(list_most_common_unrepeated).most_common(1)[0][0]
