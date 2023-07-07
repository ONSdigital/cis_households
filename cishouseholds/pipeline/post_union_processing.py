import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from cishouseholds.derive import assign_raw_copies
from cishouseholds.derive import assign_unique_id_column
from cishouseholds.edit import update_column_values_from_map


def post_union_processing(df: DataFrame):
    """"""
    df = raw_copies(df)
    df = generic_processing(df)
    return df


def raw_copies(df: DataFrame):
    raw_copy_list = [
        "think_had_covid_any_symptoms",
        "think_have_covid_symptom_any",
        "work_health_care_area",
        "work_main_job_title",
        "work_main_job_role",
        "work_status_v0",
        "work_status_v1",
        "work_status_v2",
        "work_not_from_home_days_per_week",
        "work_location",
        "sex",
        "participant_withdrawal_reason",
        "blood_sample_barcode",
        "swab_sample_barcode",
        "think_had_covid",
        "think_have_covid_onset_date",
        "other_covid_infection_test",
        "other_covid_infection_test_results",
        "think_had_covid_admitted_to_hospital",
        "think_had_covid_contacted_nhs",
        "last_covid_contact_date",
        "last_suspected_covid_contact_date",
        "contact_known_positive_covid_last_28_days",
        "contact_suspected_positive_covid_last_28_days",
        "last_covid_contact_type",
        "last_suspected_covid_contact_type",
    ]

    original_copy_list = [
        "work_health_care_patient_facing",
        "work_health_care_area",
        "work_social_care",
        "work_nursing_or_residential_care_home",
        "work_direct_contact_patients_or_clients",
        "work_patient_facing_now",
        "work_location",
        "work_status_v0",
        "work_status_v1",
        "work_status_v2",
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
    return df


def generic_processing(df: DataFrame):
    """"""
    df = assign_unique_id_column(df, "unique_participant_response_id", concat_columns=["visit_id", "participant_id"])

    df = update_column_values_from_map(
        df=df,
        column_name_to_update="smokes_nothing_now",
        map={"Yes": "No", "No": "Yes"},
        reference_column="currently_smokes_or_vapes",
    )
    return df
