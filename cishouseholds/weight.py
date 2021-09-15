from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def fill_design_weight(
    df: DataFrame,
    previous_antibody_design_weight_column: str,
    swab_design_weight_column: str,
    column_name_to_assign: str,
):
    """
    Take previous antibody design weight.
    If previous antibody design weight is missing then substitute in the swab design weight.

    Parameters
    ----------
    df
    previous_antibody_design_weight_column
    swab_design_weight_column
    column_name_to_assign
    """
    return df.withColumn(
        column_name_to_assign,
        F.coalesce(F.col(previous_antibody_design_weight_column), F.col(swab_design_weight_column)),
    )
