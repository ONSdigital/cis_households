# from typing import List
# from typing import Optional
# import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from cishouseholds.derive import assign_max_doses
from cishouseholds.derive import assign_order_number
from cishouseholds.derive import assign_pos_1_2
from cishouseholds.derive import group_participant_within_date_range
from cishouseholds.edit import update_column_values_from_map

# from pyspark.sql import Window


def vaccine_transformations(df: DataFrame):
    """"""
    df = preprocesing(df)
    return df


def preprocesing(df: DataFrame):
    """"""
    df = update_column_values_from_map(
        df,
        "cis_covid_vaccine_number_of_doses",
        {
            "1 dose": 1,
            "1": 1,
            "2 doses": 2,
            "2": 2,
            "3 doses": 3,
            "3 or more": 3,
            "4 doses": 3,
            "5 doses": 3,
            "6 doses or more": 3,
        },
    )
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
        num_doses_column="cis_covid_vaccine_number_of_doses",
        visit_datetime_column="visit_datetime",
    )
    df = assign_pos_1_2(
        df=df,
        column_name_to_assign="pos_1_2",
        i_dose_column="i_dose",
        participant_id_column="participant_id",
        num_doses_column="cis_covid_vaccine_number_of_doses",
    )
    df = assign_order_number(
        df=df,
        column_name_to_assign="order_number",
        covid_vaccine_type_column="cis_covid_vaccine_type",
        max_doses_column="max_doses",
        pos_1_2_column="pos_1_2",
    )
    return df
