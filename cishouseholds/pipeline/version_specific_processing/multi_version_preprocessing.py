# flake8: noqa
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from cishouseholds.derive import assign_work_health_care
from cishouseholds.derive import assign_work_social_column
from cishouseholds.derive import derive_household_been_columns
from cishouseholds.edit import clean_within_range


def assign_has_been_columns(df):
    df = derive_household_been_columns(
        df=df,
        column_name_to_assign="household_been_care_home_last_28_days",
        individual_response_column="care_home_last_28_days",
        household_response_column="other_household_member_care_home_last_28_days",
    )
    df = derive_household_been_columns(
        df=df,
        column_name_to_assign="household_been_hospital_last_28_days",
        individual_response_column="hospital_last_28_days",
        household_response_column="other_household_member_hospital_last_28_days",
    )
    return df


def derive_additional_v1_2_columns(df: DataFrame) -> DataFrame:
    """
    Transformations specific to the v1 and v2 survey responses.
    """
    df = clean_within_range(df, "hours_a_day_with_someone_else_at_home", [0, 24])
    df = df.withColumn("been_outside_uk_last_country", F.upper(F.col("been_outside_uk_last_country")))

    df = assign_work_social_column(
        df,
        "work_social_care",
        "work_sector",
        "work_nursing_or_residential_care_home",
        "work_direct_contact_patients_or_clients",
    )
    df = assign_work_health_care(
        df,
        "work_health_care_patient_facing",
        direct_contact_column="work_direct_contact_patients_or_clients",
        health_care_column="work_health_care_area",
    )
    return df
