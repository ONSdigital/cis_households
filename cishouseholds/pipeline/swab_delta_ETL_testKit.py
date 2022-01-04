from pyspark.sql import DataFrame

from cishouseholds.pipeline.swab_delta_ETL import transform_swab_delta


def stransform_swab_delta_testKit(df: DataFrame):
    df = transform_swab_delta(df)
    df = df.drop("testKit")
