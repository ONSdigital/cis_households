# flake8: noqa
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from cishouseholds.derive import assign_column_uniform_value
from cishouseholds.derive import assign_isin_list
from cishouseholds.derive import assign_taken_column
from cishouseholds.edit import apply_value_map_multiple_columns
from cishouseholds.edit import clean_barcode
from cishouseholds.edit import update_to_value_if_any_not_null


def clean_survey_responses_version_1(df: DataFrame) -> DataFrame:

    health_care_area_map = {
        "Primary care for example in a GP or dentist": "Primary",
        "Secondary care for example in a hospital": "Secondary",
        "Yes, in secondary care, e.g. hospital": "Secondary",
        "Yes, in other healthcare settings, e.g. mental health": "Other",
        "Yes, in primary care, e.g. GP,dentist": "Primary",
        "Another type of healthcare-for example mental health services?": "Other",
    }

    v1_times_value_map = {
        "None": 0,
        "1": 1,
        "2": 2,
        "3": 3,
        "4": 4,
        "5": 5,
        "6": 6,
        "7 times or more": 7,
        "Participant Would Not/Could Not Answer": None,
    }
    v1_column_editing_map = {
        "times_hour_or_longer_another_home_last_7_days": v1_times_value_map,
        "times_hour_or_longer_another_person_your_home_last_7_days": v1_times_value_map,
        "work_health_care_area": health_care_area_map,
    }
    df = apply_value_map_multiple_columns(df, v1_column_editing_map)
    df = df.withColumn("work_main_job_changed", F.lit(None).cast("string"))
    fill_forward_columns = [
        "work_main_job_title",
        "work_main_job_role",
        "work_sector",
        "work_sector_other",
        "work_health_care_area",
        "work_nursing_or_residential_care_home",
        "work_direct_contact_patients_or_clients",
    ]
    df = update_to_value_if_any_not_null(
        df=df,
        column_name_to_update="work_main_job_changed",
        true_false_values=["Yes", "No"],
        column_list=fill_forward_columns,
    )
    df = df.drop(
        "cis_covid_vaccine_date",
        "cis_covid_vaccine_number_of_doses",
        "cis_covid_vaccine_type",
        "cis_covid_vaccine_type_other",
        "cis_covid_vaccine_received",
    )
    return df


def transform_survey_responses_version_1_delta(df: DataFrame) -> DataFrame:
    """
    Call functions to process input for iqvia version 1 survey deltas.
    """
    df = assign_taken_column(df=df, column_name_to_assign="swab_taken", reference_column="swab_sample_barcode")
    df = assign_taken_column(df=df, column_name_to_assign="blood_taken", reference_column="blood_sample_barcode")

    df = assign_column_uniform_value(df, "survey_response_dataset_major_version", 1)

    df = df.withColumn("work_status_v0", F.col("work_status_v1"))
    df = df.withColumn("work_status_v2", F.col("work_status_v1"))

    been_value_map = {"No, someone else in my household has": "No I haven’t, but someone else in my household has"}
    column_editing_map = {
        "work_status_v0": {
            "Employed and currently working": "Employed",  # noqa: E501
            "Employed and currently not working": "Furloughed (temporarily not working)",  # noqa: E501
            "Self-employed and currently not working": "Furloughed (temporarily not working)",  # noqa: E501
            "Retired": "Not working (unemployed, retired, long-term sick etc.)",  # noqa: E501
            "Looking for paid work and able to start": "Not working (unemployed, retired, long-term sick etc.)",
            # noqa: E501
            "Not working and not looking for work": "Not working (unemployed, retired, long-term sick etc.)",
            # noqa: E501
            "Child under 5y not attending child care": "Student",  # noqa: E501
            "Child under 5y attending child care": "Student",  # noqa: E501
            "5y and older in full-time education": "Student",  # noqa: E501
            "Self-employed and currently working": "Self-employed",  # noqa: E501
        },
        "work_status_v2": {
            "Child under 5y not attending child care": "Child under 4-5y not attending child care",  # noqa: E501
            "Child under 5y attending child care": "Child under 4-5y attending child care",  # noqa: E501
            "5y and older in full-time education": "4-5y and older at school/home-school",  # noqa: E501
        },
        "household_been_hospital_last_28_days": been_value_map,
        "household_been_care_home_last_28_days": been_value_map,
        "times_outside_shopping_or_socialising_last_7_days": {
            "None": 0,
            "1": 1,
            "2": 2,
            "3": 3,
            "4": 4,
            "5": 5,
            "6": 6,
            "7 times or more": 7,
        },
    }

    df = assign_isin_list(
        df=df,
        column_name_to_assign="self_isolating",
        reference_column="self_isolating_reason",
        values_list=[
            "Yes, for other reasons (e.g. going into hospital, quarantining)",
            "Yes, you have/have had symptoms",
            "Yes, someone you live with had symptoms",
        ],
        true_false_values=["Yes", "No"],
    )
    df = apply_value_map_multiple_columns(df, column_editing_map)
    df = clean_barcode(df=df, barcode_column="swab_sample_barcode", edited_column="swab_sample_barcode_edited_flag")
    df = clean_barcode(df=df, barcode_column="blood_sample_barcode", edited_column="blood_sample_barcode_edited_flag")
    return df
