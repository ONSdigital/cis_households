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


def high_level_phm_transformations(df: DataFrame, participant_data_df: DataFrame = None):
    """"""

    if participant_data_df:
        df = df.join(participant_data_df, on="participant_id", how="left")
    df = preprocessing(df)
    return df


def preprocessing(df: DataFrame):
    """"""

    return df
