# from typing import List
# from typing import Optional
# import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from cishouseholds.derive import assign_max_doses
from cishouseholds.derive import assign_pos_1_2
from cishouseholds.derive import group_participant_within_date_range

# from pyspark.sql import Window


def vaccine_transformations(df: DataFrame):
    """"""
    df = preprocesing(df)
    return df


def preprocesing(df: DataFrame):
    """"""
    df = group_participant_within_date_range(
        df=df,
        column_name_to_assign="i_dose",
        participant_id_column="participant_id",
        date_column="cis_covid_vaccine_date",
        date_range=16,
    )
    df = assign_max_doses(
        df=df,
        column_name_to_assign="max_doses",
        i_dose_column="i_dose",
        participant_id_column="participant_id",
        num_doses_column="covid_vaccine_n_doses",
    )
    df = assign_pos_1_2(
        df=df,
        column_name_to_assign="pos_1_2",
        i_dose_column="i_dose",
        participant_id_column="participant_id",
        num_doses_column="covid_vaccine_n_doses",
    )

    return df
