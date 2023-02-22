# from typing import List
# from typing import Optional
# import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from cishouseholds.derive import assign_max_1_2_doses

# from pyspark.sql import Window


def vaccination_transformations(df: DataFrame):
    """"""
    df = deduplication(df)
    return df


def deduplication(df: DataFrame):
    """"""
    df = assign_max_1_2_doses(
        df,
        column_name_to_assign="max_1_2_doses",
        participant_id_column="participant_id",
        vaccine_date_column="cis_covid_vaccine_date",
        num_doses_column="cis_covid_vaccine_num_doses",
    )
    return df
