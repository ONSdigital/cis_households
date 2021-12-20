from pyspark.sql import DataFrame

from cishouseholds.derive import assign_filename_column


def transform_unassayed_blood(df: DataFrame) -> DataFrame:
    """
    Call functions to process input for unassayed blood.
    """
    df = assign_filename_column(df, "unassayed_blood_source_file")
    return df
