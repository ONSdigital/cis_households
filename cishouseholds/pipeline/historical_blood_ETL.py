from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame


def add_fields(df: DataFrame):
    """Add fields that might be missing in example data."""
    new_columns = {
        "antibody_test_undiluted_result_value": "float",
        "antibody_test_bounded_result_value": "float",
    }
    for column, type in new_columns.items():
        if column not in df.columns:
            df = df.withColumn(column, F.lit(None).cast(type))
    df = df.select(sorted(df.columns))
    return df
