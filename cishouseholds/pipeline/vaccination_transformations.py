# from typing import List
# from typing import Optional
# import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from cishouseholds.filter import filter_invalid_vaccines

# from pyspark.sql import Window


def vaccination_transformations(df: DataFrame):
    """"""
    df = deduplication(df)
    return df


def deduplication(df: DataFrame):
    """"""
    df = filter_invalid_vaccines(
        df=df,
        participant_id_column="participant_id",
        vaccine_date_column="cis_covid_vaccine_date",
        num_doses_column="cis_covid_vaccine_num_doses",
        visit_datetime_column="visit_datetime",
    )
    return df
