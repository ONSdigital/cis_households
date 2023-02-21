# flake8: noqa
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.dataframe import DataFrame

from cishouseholds.edit import update_column_values_from_map
from cishouseholds.filter import filter_before_date_or_null


def phm_transformations(df: DataFrame):
    df = preprocessing(df)
    return df


def preprocessing(df: DataFrame):
    df = filter_before_date_or_null(df, "cis_covid_vaccine_date", "2020-12-01")
    df = update_column_values_from_map(
        df, map={None: "don't know type", "Other / specify": "don't know type"}, column="cis_covid_vaccine_date"
    )
    return df
