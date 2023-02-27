# flake8: noqa
# from pyspark.sql import Window
# from typing import List
# from typing import Optional
# import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from cishouseholds.derive import assign_datetime_from_combined_columns

# from pyspark.sql import Window


def phm_transformations(df: DataFrame):
    """"""
    df = preprocessing(df)
    return df


def preprocessing(df: DataFrame):
    """"""
    df = assign_datetime_from_combined_columns(
        df=df,
        column_name_to_assign="blood_taken_datetime",
        date_column="blood_taken_date",
        hour_column="blood_taken_time_hour",
        minute_column="blood_taken_time_minute",
        am_pm_column="blood_taken_am_pm",
    )
    df = assign_datetime_from_combined_columns(
        df=df,
        column_name_to_assign="swab_taken_datetime",
        date_column="blood_taken_date",
        hour_column="blood_taken_time_hour",
        minute_column="blood_taken_time_minute",
        am_pm_column="blood_taken_am_pm",
    )
    return df
