from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from cishouseholds.derive import assign_column_uniform_value
from cishouseholds.edit import apply_value_map_multiple_columns
from cishouseholds.edit import clean_barcode
from cishouseholds.edit import map_column_values_to_null
from cishouseholds.edit import update_to_value_if_any_not_null


def clean_survey_responses_version_1(df: DataFrame) -> DataFrame:
    df = map_column_values_to_null(
        df=df,
        value="Participant Would Not/Could Not Answer",
        column_list=[
            "ethnicity",
            "work_sector",
            "work_health_care_area",
            "work_status_v1",
            "work_location",
            "work_direct_contact_patients_or_clients",
            "survey_response_type",
            "self_isolating_reason",
            "illness_reduces_activity_or_ability",
            "ability_to_socially_distance_at_work_or_school",
            "transport_to_work_or_education",
            "face_covering_outside_of_home",
            "other_antibody_test_location",
            "participant_withdrawal_reason",
            "work_not_from_home_days_per_week",
        ],
    )

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
        column_name_to_assign="work_main_job_changed",
        value_to_assign="Yes",
        column_list=fill_forward_columns,
    )
    return df


def transform_survey_responses_version_1_delta(df: DataFrame) -> DataFrame:
    """
    Call functions to process input for iqvia version 1 survey deltas.
    """
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
            "Looking for paid work and able to start": "Not working (unemployed, retired, long-term sick etc.)",  # noqa: E501
            "Not working and not looking for work": "Not working (unemployed, retired, long-term sick etc.)",  # noqa: E501
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
    df = apply_value_map_multiple_columns(df, column_editing_map)
    df = clean_barcode(df=df, barcode_column="swab_sample_barcode", edited_column="swab_sample_barcode_edited_flag")
    df = clean_barcode(df=df, barcode_column="blood_sample_barcode", edited_column="blood_sample_barcode_edited_flag")
    return df
