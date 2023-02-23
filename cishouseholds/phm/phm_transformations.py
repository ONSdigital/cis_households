# flake8: noqa
# from pyspark.sql import functions as F
# from pyspark.sql import Window
from pyspark.sql.dataframe import DataFrame


def phm_transformations(df: DataFrame):
    df = preprocessing(df)
    return df


def preprocessing(df: DataFrame):
    return df
