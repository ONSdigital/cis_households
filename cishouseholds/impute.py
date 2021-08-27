import sys
from collections import Counter
from typing import Any
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


def calculate_imputation_from_mode(
    df: DataFrame, column_name_to_assign: str, reference_column: str, group_column: str
) -> DataFrame:
    """
    Get imputation value from given column by most repeated UNIQUE value
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
    # get rid of cases (household_id) with no nulls to be imputed and only show the cases
    df_group_imput = df.filter(F.col(reference_column).isNull()).select(F.col(group_column))

    # naming convention
    list_reference_column = "list_" + reference_column

    # window function to collect_list() of all the reference_column (ethnicity) items
    # per group (household_id)
    window = Window.partitionBy(group_column)
    df_window = (
        df.withColumn(list_reference_column, F.collect_list(F.col(reference_column)).over(window))
        .filter(F.col(reference_column).isNotNull())
        .dropDuplicates([group_column, list_reference_column])
        .drop(reference_column)
    )

    # JOIN 1: List of households with imputation (Nulls) to impute inner joined to
    # the previous df_window that has the list of items in reference_column (ethnicity)
    df_group_imput = df_group_imput.join(df_window, [group_column], "inner")

    # Function to implement as UDF - User defined function to workout the logic for imputation
    udf_most_common_unique = F.udf(lambda x: most_common_unique_item(x))

    # UDF implementation: with most_common_unique_item() inputation per row is performed
    df_imputed = df_group_imput.withColumn(
        column_name_to_assign, udf_most_common_unique(F.col(list_reference_column))
    ).drop(list_reference_column)

    # JOIN 2: from initial input df outer join it with the imputed values with UDF
    # df_imputed with group_column (household_id) column
    actual_df = df.join(df_imputed, [group_column], "outer")

    # Only impute when the cell of the imputed column is null
    actual_df = actual_df.withColumn(
        column_name_to_assign,
        F.when((F.col(reference_column).isNotNull()), None).otherwise(F.col(column_name_to_assign)),
    )
    return actual_df


def most_common_unique_item(list_most_common_unrepeated: List[str]) -> Any:
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
