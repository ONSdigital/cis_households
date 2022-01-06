from pyspark.sql import DataFrame


def transform_swab_delta_testKit(df: DataFrame):
    df = df.drop("testKit")

    return df
