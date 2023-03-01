# from typing import List
# from typing import Optional
# import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from cishouseholds.derive import assign_column_value_from_multiple_column_map
from cishouseholds.derive import assign_default_date_flag
from cishouseholds.derive import assign_max_doses
from cishouseholds.derive import assign_order_number
from cishouseholds.derive import assign_poss_1_2
from cishouseholds.derive import group_participant_within_date_range
from cishouseholds.edit import update_column_values_from_map
from cishouseholds.filter import filter_before_date_or_null
from cishouseholds.filter import filter_invalid_vaccines
from cishouseholds.filter import filter_single_dose


# from pyspark.sql import Window


def vaccine_transformations(df: DataFrame):
    """"""
    df = mapping(df)
    df = preprocessing(df)
    df = deduplication(df)
    return df


def mapping(df: DataFrame):
    """"""
    df = filter_before_date_or_null(df, "cis_covid_vaccine_date", "2020-12-01")
    df = update_column_values_from_map(
        df,
        map={None: "don't know type", "Other / specify": "don't know type"},
        column="cis_covid_vaccine_type",
        condition_expression=(F.col("cis_covid_vaccine_date").isNotNull()),
    )
    df = assign_default_date_flag(df, "cis_covid_vaccine_date", default_days=[1, 15])
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
    return df


def preprocessing(df: DataFrame):
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
    df = assign_poss_1_2(
        df=df,
        column_name_to_assign="pos_1_2",
        participant_id_column="participant_id",
        num_doses_column="cis_covid_vaccine_number_of_doses",
        visit_datetime_column="visit_datetime",
    )
    df = assign_order_number(
        df=df,
        column_name_to_assign="order_number",
        covid_vaccine_type_column="cis_covid_vaccine_type",
        num_doses_column="cis_covid_vaccine_number_of_doses",
        max_doses_column="max_doses",
        pos_1_2_column="pos_1_2",
    )
    df = assign_column_value_from_multiple_column_map(
        df=df,
        column_name_to_assign="cis_covid_vaccine_type",
        value_to_condition_map=[
            ["Don't know type", [[4, 5], "Yes", "No"]],
        ],
        column_names=["order", "poss_1_2", "max_doses"],
        override_original=False,
    )
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
    df = filter_single_dose(
        df=df,
        participant_id_column="participant_id",
        visit_datetime_column="visit_datetime",
        order_column="order_number",
        i_dose_column="i_dose",
        poss_1_2_column="poss_1_2",
        default_date_column="default_date",
        vaccine_type_column="cis_covid_vaccine_type",
        allowed_vaccine_types=[
            "Oxford/AstraZeneca",
            "Pfizer/BioNTech",
            "Moderna",
            "Oxford / AstraZeneca / Vaxzevria / Covishield",
        ],
    )
    return df
