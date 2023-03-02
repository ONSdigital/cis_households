# flake8: noqa
# from pyspark.sql import Window
# from typing import List
# from typing import Optional
# import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from cishouseholds.derive import assign_column_uniform_value
from cishouseholds.derive import assign_completion_status
from cishouseholds.derive import assign_date_from_filename
from cishouseholds.derive import assign_datetime_from_combined_columns
from cishouseholds.edit import add_prefix

# from pyspark.sql import Window


def high_level_phm_transformations(df: DataFrame):
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
    df = assign_column_uniform_value(df, "survey_response_dataset_major_version", 4)
    df = assign_completion_status(df=df, column_name_to_assign="survey_completion_status")
    df = add_prefix(df, column_name_to_update="blood_sample_barcode_user_entered", prefix="BLT")
    df = add_prefix(df, column_name_to_update="swab_sample_barcode_user_entered", prefix="SWT")
    return df
