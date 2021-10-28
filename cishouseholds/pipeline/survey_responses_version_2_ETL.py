from pyspark.sql import DataFrame

from cishouseholds.derive import assign_age_at_date
from cishouseholds.derive import assign_column_regex_match
from cishouseholds.derive import assign_column_to_date_string
from cishouseholds.derive import assign_column_uniform_value
from cishouseholds.derive import assign_consent_code
from cishouseholds.derive import assign_filename_column
from cishouseholds.derive import assign_named_buckets
from cishouseholds.derive import assign_outward_postcode
from cishouseholds.derive import assign_school_year_september_start
from cishouseholds.derive import assign_taken_column
from cishouseholds.derive import assign_work_patient_facing_now
from cishouseholds.edit import convert_barcode_null_if_zero
from cishouseholds.edit import convert_null_if_not_in_list
from cishouseholds.edit import format_string_upper_and_clean
from cishouseholds.edit import update_work_facing_now_column
from cishouseholds.pipeline.ETL_scripts import extract_validate_transform_input_data
from cishouseholds.pipeline.input_variable_names import survey_responses_v2_variable_name_map
from cishouseholds.pipeline.load import update_table_and_log_source_files
from cishouseholds.pipeline.pipeline_stages import register_pipeline_stage
from cishouseholds.pipeline.timestamp_map import survey_responses_v2_datetime_map
from cishouseholds.pipeline.validation_schema import survey_responses_v2_validation_schema

# from cishouseholds.derive import assign_work_person_facing_now


@register_pipeline_stage("survey_responses_version_2_ETL")
def survey_responses_version_2_ETL(resource_path: str):
    """
    End to end processing of a IQVIA survey responses CSV file.
    """
    df = extract_validate_transform_input_data(
        resource_path,
        survey_responses_v2_variable_name_map,
        survey_responses_v2_datetime_map,
        survey_responses_v2_validation_schema,
        transform_survey_responses_version_2_delta,
    )
    update_table_and_log_source_files(df, "transformed_survey_responses_v2_data", "survey_responses_v2_source_file")
    return df


def transform_survey_responses_version_2_delta(df: DataFrame) -> DataFrame:
    """
    Call functions to process input for iqvia version 2 survey deltas.
    """
    df = assign_filename_column(df, "survey_responses_v2_source_file")
    df = assign_column_uniform_value(df, "survey_response_dataset_major_version", 1)
    df = assign_column_regex_match(df, "bad_email", "email", r"/^w+[+.w-]*@([w-]+.)*w+[w-]*.([a-z]{2,4}|d+)$/i")
    df = assign_column_to_date_string(df, "visit_date_string", "visit_datetime")
    df = assign_column_to_date_string(df, "sample_taken_date_string", "samples_taken_datetime")
    df = assign_column_to_date_string(df, "date_of_birth_string", "date_of_birth")
    df = assign_column_uniform_value(df, "dataset", 1)  # replace 'n' with chosen value
    df = convert_barcode_null_if_zero(df, "swab_sample_barcode")
    df = convert_barcode_null_if_zero(df, "blood_sample_barcode")
    df = assign_taken_column(df, "swab_taken", "swab_sample_barcode")
    df = assign_taken_column(df, "blood_taken", "blood_sample_barcode")
    df = format_string_upper_and_clean(df, "work_main_job_title")
    df = format_string_upper_and_clean(df, "work_main_job_role")
    df = convert_null_if_not_in_list(df, "sex", ["Male", "Female"])
    # df = placeholder_for_derivation_number_7-2(df, "week")
    # derviation number 7 has been used twice - currently associated to ctpatterns
    # df = placeholder_for_derivation_number_7-2(df, "month")
    df = assign_outward_postcode(
        df, "outward_postcode", "postcode"
    )  # splits on space between postcode segments and gets left half
    # df = placeholder_for_derivation_number_17(df, "country_barcode", ["swab_barcode_cleaned","blood_barcode_cleaned"],
    #  {0:"ONS", 1:"ONW", 2:"ONN", 3:"ONC"})
    # df = placeholder_for_derivation_number_18(df, "hh_id_fake",	"ons_household_id")
    df = assign_consent_code(df, "consent", ["consent_16_visits", "consent_5_visits", "consent_1_visit"])
    # df = placeholder_for_derivation_number_20(df, "work_healthcare",
    # ["work_healthcare_v1", "work_direct_contact"])
    # df = placeholder_for_derivation_number_21(df, "work_socialcare",
    # ["work_sector", "work_care_nursing_home" , "work_direct_contact"])
    # df = placeholder_for_derivation_number_10(df, "self_isolating", "self_isolating_v1")
    # df = placeholder_for_derivation_number_10(df, "contact_hospital",
    # ["contact_participant_hospital", "contact_other_in_hh_hospital"])
    # df = placeholder_for_derivation_number_10(df, "contact_carehome",
    # ["contact_participant_carehome", "contact_other_in_hh_carehome"])
    df = assign_age_at_date(df, "age_at_visit", "visit_datetime", "date_of_birth")
    df = assign_named_buckets(
        df, "age_at_visit", "age_group_5_intervals", {2: "2-11", 12: "12-19", 20: "20-49", 50: "50-69", 70: "70+"}
    )
    df = assign_named_buckets(df, "age_at_visit", "age_group_over_16", {16: "16-49", 50: "50-70", 70: "70+"})
    df = assign_named_buckets(
        df,
        "age_at_visit",
        "age_group_7_intervals",
        {2: "2-11", 12: "12-16", 17: "17-25", 25: "25-34", 35: "35-49", 50: "50-69", 70: "70+"},
    )
    df = assign_named_buckets(
        df,
        "age_at_visit",
        "age_group_5_year_intervals",
        {
            2: "2-4",
            5: "5-9",
            10: "10-14",
            15: "15-19",
            20: "20-24",
            25: "25-29",
            30: "30-34",
            35: "35-39",
            40: "40-44",
            45: "45-49",
            50: "50-54",
            55: "55-59",
            60: "60-64",
            65: "65-69",
            70: "70-74",
            75: "75-79",
            80: "80-84",
            85: "85-89",
            90: "90+",
        },
    )
    df = assign_school_year_september_start(df, "date_of_birth", "visit_datetime", "school_year_september")
    df = assign_work_patient_facing_now(df, "work_patient_facing_now", "age_at_visit", "work_health_care")
    # df = assign_work_person_facing_now(df, "work_person_facing_now", "work_person_facing_now", "work_social_care")
    df = update_work_facing_now_column(
        df,
        "work_patient_facing_now",
        "work_status",
        ["Furloughed (temporarily not working)", "Not working (unemployed, retired, long-term sick etc.)", "Student"],
    )
    # df = update_work_facing_now_column(
    #     df,
    #     "work_person_facing_now",
    #     "work_status",
    #     ["Furloughed (temporarily not working)", "Not working (unemployed, retired, long-term sick etc.)", "Student"],
    # )
    # df = placeholder_for_derivation_number_23(df, "work_status", ["work_status_v1", "work_status_v2"])
    return df
