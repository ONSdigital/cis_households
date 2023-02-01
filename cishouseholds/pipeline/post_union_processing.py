from functools import reduce
from operator import or_

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from cishouseholds.derive import assign_date_from_filename
from cishouseholds.derive import assign_raw_copies
from cishouseholds.derive import assign_unique_id_column
from cishouseholds.edit import apply_value_map_multiple_columns
from cishouseholds.edit import correct_date_ranges
from cishouseholds.edit import map_column_values_to_null
from cishouseholds.pipeline.mapping import date_cols_min_date_dict


def raw_copies():
    raw_copy_list = [
        "think_had_covid_any_symptoms",
        "think_have_covid_symptom_any",
        "work_health_care_area",
        "work_main_job_title",
        "work_main_job_role",
        "work_status_v1",
        "work_status_v2",
        "work_not_from_home_days_per_week",
        "work_location",
        "sex",
        "participant_withdrawal_reason",
        "blood_sample_barcode",
        "swab_sample_barcode",
    ]
    original_copy_list = [
        "work_health_care_patient_facing",
        "work_health_care_area",
        "work_social_care",
        "work_nursing_or_residential_care_home",
        "work_direct_contact_patients_or_clients",
        "work_patient_facing_now",
    ]

    upper_cols = [
        "cis_covid_vaccine_type_other",
        "cis_covid_vaccine_type_other_1",
        "cis_covid_vaccine_type_other_2",
        "cis_covid_vaccine_type_other_3",
        "cis_covid_vaccine_type_other_4",
        "cis_covid_vaccine_type_other_5",
        "cis_covid_vaccine_type_other_6",
    ]

    for col in upper_cols:
        if col in df.columns:
            df = df.withColumn(col, F.upper(F.col(col)))

    df = assign_raw_copies(df, [column for column in raw_copy_list if column in df.columns])

    df = assign_raw_copies(df, [column for column in original_copy_list if column in df.columns], "original")


def date_corrections(df: DataFrame):
    """"""
    date_cols_to_correct = [
        col
        for col in [
            "last_covid_contact_date",
            "last_suspected_covid_contact_date",
            "think_had_covid_onset_date",
            "think_have_covid_onset_date",
            "been_outside_uk_last_return_date",
            "other_covid_infection_test_first_positive_date",
            "other_covid_infection_test_last_negative_date",
            "other_antibody_test_first_positive_date",
            "other_antibody_test_last_negative_date",
        ]
        if col in df.columns
    ]

    df = assign_raw_copies(df, date_cols_to_correct, "pdc")
    df = correct_date_ranges(df, date_cols_to_correct, "visit_datetime", "2019-08-01", date_cols_min_date_dict)
    df = df.withColumn(
        "any_date_corrected",
        F.when(reduce(or_, [~F.col(col).eqNullSafe(F.col(f"{col}_pdc")) for col in date_cols_to_correct]), "Yes"),
    )
    df = df.drop(*[f"{col}_pdc" for col in date_cols_to_correct])
    return df


def generic_processing(df: DataFrame):
    """"""
    df = assign_unique_id_column(df, "unique_participant_response_id", concat_columns=["visit_id", "participant_id"])
    df = assign_date_from_filename(df, "file_date", "survey_response_source_file")
    df = df.withColumn("hh_id", F.col("ons_household_id"))
    dont_know_columns = [
        "work_in_additional_paid_employment",
        "work_nursing_or_residential_care_home",
        "work_direct_contact_patients_or_clients",
        "self_isolating",
        "illness_lasting_over_12_months",
        "ever_smoked_regularly",
        "currently_smokes_or_vapes",
        "cis_covid_vaccine_type_1",
        "cis_covid_vaccine_type_2",
        "cis_covid_vaccine_type_3",
        "cis_covid_vaccine_type_4",
        "cis_covid_vaccine_type_5",
        "cis_covid_vaccine_type_6",
        "other_household_member_hospital_last_28_days",
        "other_household_member_care_home_last_28_days",
        "hours_a_day_with_someone_else_at_home",
        "physical_contact_under_18_years",
        "physical_contact_18_to_69_years",
        "physical_contact_over_70_years",
        "social_distance_contact_under_18_years",
        "social_distance_contact_18_to_69_years",
        "social_distance_contact_over_70_years",
        "times_hour_or_longer_another_home_last_7_days",
        "times_hour_or_longer_another_person_your_home_last_7_days",
        "times_shopping_last_7_days",
        "times_socialising_last_7_days",
        "face_covering_work_or_education",
        "face_covering_other_enclosed_places",
        "cis_covid_vaccine_type",
    ]
    df = assign_raw_copies(df, dont_know_columns)
    dont_know_mapping_dict = {
        "Prefer not to say": None,
        "Don't know": None,
        "Don't Know": None,
        "I don't know the type": "Don't know type",
        "Dont know": None,
        "Don&#39;t know": None,
        "Do not know": None,
        "Don&amp;#39;t Know": None,
    }
    df = apply_value_map_multiple_columns(
        df,
        {k: dont_know_mapping_dict for k in dont_know_columns},
    )
    df = map_column_values_to_null(
        df=df,
        value="Participant Would Not/Could Not Answer",
        column_list=[
            "ethnicity",
            "work_sector",
            "work_health_care_area",
            "household_visit_status",
            "participant_survey_status",
            "work_status_v0",
            "work_status_v1",
            "work_status_v2",
            "work_location",
            "face_covering_work_or_education",
            "face_covering_other_enclosed_places",
            "cis_covid_vaccine_type",
            "cis_covid_vaccine_number_of_doses",
            "times_shopping_last_7_days",
            "times_socialising_last_7_days",
            "physical_contact_under_18_years",
            "physical_contact_18_to_69_years",
            "physical_contact_over_70_years",
            "social_distance_contact_under_18_years",
            "social_distance_contact_18_to_69_years",
            "social_distance_contact_over_70_years",
            "hospital_last_28_days",
            "care_home_last_28_days",
            "other_household_member_care_home_last_28_days",
            "other_household_member_hospital_last_28_days",
            "think_have_covid",
            "work_direct_contact_patients_or_clients",
            "work_nursing_or_residential_care_home",
            "survey_response_type",
            "self_isolating_reason",
            "illness_reduces_activity_or_ability",
            "ability_to_socially_distance_at_work_or_education",
            "transport_to_work_or_education",
            "face_covering_outside_of_home",
            "other_antibody_test_location",
            "participant_withdrawal_reason",
            "work_not_from_home_days_per_week",
        ],
    )