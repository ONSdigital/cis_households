# flake8: noqa
from functools import reduce
from operator import or_

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame

from cishouseholds.derive import assign_column_from_mapped_list_key
from cishouseholds.derive import assign_column_regex_match
from cishouseholds.derive import assign_column_uniform_value
from cishouseholds.derive import assign_column_value_from_multiple_column_map
from cishouseholds.derive import assign_consent_code
from cishouseholds.derive import assign_date_difference
from cishouseholds.derive import assign_date_from_filename
from cishouseholds.derive import assign_datetime_from_coalesced_columns_and_log_source
from cishouseholds.derive import assign_grouped_variable_from_days_since
from cishouseholds.derive import assign_isin_list
from cishouseholds.derive import assign_raw_copies
from cishouseholds.derive import assign_taken_column
from cishouseholds.derive import assign_unique_id_column
from cishouseholds.derive import assign_work_health_care
from cishouseholds.derive import assign_work_social_column
from cishouseholds.derive import concat_fields_if_true
from cishouseholds.derive import derive_had_symptom_last_7days_from_digital
from cishouseholds.derive import derive_household_been_columns
from cishouseholds.derive import map_options_to_bool_columns
from cishouseholds.edit import apply_value_map_multiple_columns
from cishouseholds.edit import assign_from_map
from cishouseholds.edit import clean_barcode
from cishouseholds.edit import clean_barcode_simple
from cishouseholds.edit import clean_job_description_string
from cishouseholds.edit import clean_within_range
from cishouseholds.edit import conditionally_set_column_values
from cishouseholds.edit import correct_date_ranges
from cishouseholds.edit import edit_to_sum_or_max_value
from cishouseholds.edit import map_column_values_to_null
from cishouseholds.edit import normalise_think_had_covid_columns
from cishouseholds.edit import survey_edit_auto_complete
from cishouseholds.edit import update_column_in_time_window
from cishouseholds.edit import update_column_values_from_map
from cishouseholds.edit import update_strings_to_sentence_case
from cishouseholds.edit import update_to_value_if_any_not_null
from cishouseholds.edit import update_value_if_multiple_and_ref_in_list
from cishouseholds.pipeline.config import get_config
from cishouseholds.pipeline.input_file_processing import extract_lookup_csv
from cishouseholds.pipeline.mapping import date_cols_min_date_dict
from cishouseholds.pipeline.post_union_transformations import create_formatted_datetime_string_columns
from cishouseholds.pipeline.translate import backup_and_replace_translation_lookup_df
from cishouseholds.pipeline.translate import export_responses_to_be_translated_to_translation_directory
from cishouseholds.pipeline.translate import get_new_translations_from_completed_translations_directory
from cishouseholds.pipeline.translate import get_welsh_responses_to_be_translated
from cishouseholds.pipeline.translate import translate_welsh_fixed_text_responses_digital
from cishouseholds.pipeline.translate import translate_welsh_free_text_responses_digital
from cishouseholds.pipeline.validation_schema import validation_schemas  # noqa: F401


def transform_participant_extract_digital(df: DataFrame) -> DataFrame:
    """
    transform and process participant extract data received from cis digital
    """
    col_val_map = {
        "participant_withdrawal_reason": {
            "Moving Location": "Moving location",
            "Bad experience with tester / survey": "Bad experience with interviewer/survey",
            "Swab / blood process too distressing": "Swab/blood process too distressing",
            "Do NOT Reinstate": "",
        },
        "ethnicity": {
            "African": "Black,Caribbean,African-African",
            "Caribbean": "Black,Caribbean,Afro-Caribbean",
            "Any other Black or African or Carribbean background": "Any other Black background",
            "Any other Mixed/Multiple background": "Any other Mixed background",
            "Bangladeshi": "Asian or Asian British-Bangladeshi",
            "Chinese": "Asian or Asian British-Chinese",
            "English, Welsh, Scottish, Northern Irish or British": "White-British",
            "Indian": "Asian or Asian British-Indian",
            "Irish": "White-Irish",
            "Pakistani": "Asian or Asian British-Pakistani",
            "White and Asian": "Mixed-White & Asian",
            "White and Black African": "Mixed-White & Black African",
            "White and Black Caribbean": "Mixed-White & Black Caribbean",
            "Gypsy or Irish Traveller": "White-Gypsy or Irish Traveller",
            "Arab": "Other ethnic group-Arab",
        },
    }
    ethnic_group_map = {
        "White": ["White-British", "White-Irish", "White-Gypsy or Irish Traveller", "Any other white background"],
        "Asian": [
            "Asian or Asian British-Indian",
            "Asian or Asian British-Pakistani",
            "Asian or Asian British-Bangladeshi",
            "Asian or Asian British-Chinese",
            "Any other Asian background",
        ],
        "Black": ["Black,Caribbean,African-African", "Black,Caribbean,Afro-Caribbean", "Any other Black background"],
        "Mixed": [
            "Mixed-White & Black Caribbean",
            "Mixed-White & Black African",
            "Mixed-White & Asian",
            "Any other Mixed background",
        ],
        "Other": ["Other ethnic group-Arab", "Any other ethnic group"],
    }

    df = assign_column_from_mapped_list_key(
        df=df, column_name_to_assign="ethnicity_group", reference_column="ethnic_group", map=ethnic_group_map
    )

    df = apply_value_map_multiple_columns(df, col_val_map)
    df = create_formatted_datetime_string_columns(df)

    return df


def transform_survey_responses_version_0_delta(df: DataFrame) -> DataFrame:
    """
    Call functions to process input for iqvia version 0 survey deltas.
    """
    df = assign_taken_column(df=df, column_name_to_assign="swab_taken", reference_column="swab_sample_barcode")
    df = assign_taken_column(df=df, column_name_to_assign="blood_taken", reference_column="blood_sample_barcode")

    df = assign_column_uniform_value(df, "survey_response_dataset_major_version", 0)
    invalid_covid_date = "2019-11-17"
    v0_condition = (
        (F.col("survey_response_dataset_major_version") == 0)
        & (F.col("think_had_covid_onset_date").isNotNull())
        & (F.col("think_had_covid_onset_date") < invalid_covid_date)
    )
    v0_value_map = {
        "other_covid_infection_test": None,
        "other_covid_infection_test_results": None,
    }
    df = conditionally_set_column_values(
        df=df,
        condition=v0_condition,
        cols_to_set_to_value=v0_value_map,
    )
    df = df.withColumn("sex", F.coalesce(F.col("sex"), F.col("gender"))).drop("gender")

    # Create before editing to v1 version below
    df = df.withColumn("work_health_care_area", F.col("work_health_care_patient_facing"))

    column_editing_map = {
        "work_health_care_area": {
            "Yes, primary care, patient-facing": "Primary",
            "Yes, secondary care, patient-facing": "Secondary",
            "Yes, other healthcare, patient-facing": "Other",
            "Yes, primary care, non-patient-facing": "Primary",
            "Yes, secondary care, non-patient-facing": "Secondary",
            "Yes, other healthcare, non-patient-facing": "Other",
        },
        "work_location": {
            "Both (working from home and working outside of your home)": "Both (from home and somewhere else)",
            "Working From Home": "Working from home",
            "Working Outside of your Home": "Working somewhere else (not your home)",
            "Not applicable": "Not applicable, not currently working",
        },
        "last_covid_contact_type": {
            "In your own household": "Living in your own home",
            "Outside your household": "Outside your home",
        },
        "last_suspected_covid_contact_type": {
            "In your own household": "Living in your own home",
            "Outside your household": "Outside your home",
        },
        "other_covid_infection_test_results": {
            "Positive": "One or more positive test(s)",
            "Negative": "Any tests negative, but none positive",
        },
    }
    df = apply_value_map_multiple_columns(df, column_editing_map)

    df = clean_barcode(df=df, barcode_column="swab_sample_barcode", edited_column="swab_sample_barcode_edited_flag")
    df = clean_barcode(df=df, barcode_column="blood_sample_barcode", edited_column="blood_sample_barcode_edited_flag")
    df = df.drop(
        "cis_covid_vaccine_date",
        "cis_covid_vaccine_number_of_doses",
        "cis_covid_vaccine_type",
        "cis_covid_vaccine_type_other",
        "cis_covid_vaccine_received",
    )
    return df


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
        column_name_to_assign="work_main_job_changed",
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


def clean_survey_responses_version_2(df: DataFrame) -> DataFrame:

    # Map to digital from raw V2 values, before editing them to V1 below
    df = assign_from_map(
        df,
        "self_isolating_reason_detailed",
        "self_isolating_reason",
        {
            "Yes for other reasons (e.g. going into hospital or quarantining)": "Due to increased risk of getting COVID-19 such as having been in contact with a known case or quarantining after travel abroad",
            # noqa: E501
            "Yes for other reasons related to reducing your risk of getting COVID-19 (e.g. going into hospital or shielding)": "Due to reducing my risk of getting COVID-19 such as going into hospital or shielding",
            # noqa: E501
            "Yes for other reasons related to you having had an increased risk of getting COVID-19 (e.g. having been in contact with a known case or quarantining after travel abroad)": "Due to increased risk of getting COVID-19 such as having been in contact with a known case or quarantining after travel abroad",
            # noqa: E501
            "Yes because you live with someone who has/has had symptoms but you haven’t had them yourself": "I haven't had any symptoms but I live with someone who has or has had symptoms or a positive test",
            # noqa: E501
            "Yes because you live with someone who has/has had symptoms or a positive test but you haven’t had symptoms yourself": "I haven't had any symptoms but I live with someone who has or has had symptoms or a positive test",
            # noqa: E501
            "Yes because you live with someone who has/has had symptoms but you haven't had them yourself": "I haven't had any symptoms but I live with someone who has or has had symptoms or a positive test",
            # noqa: E501
            "Yes because you have/have had symptoms of COVID-19": "I have or have had symptoms of COVID-19 or a positive test",
            "Yes because you have/have had symptoms of COVID-19 or a positive test": "I have or have had symptoms of COVID-19 or a positive test",
        },
    )

    v2_times_value_map = {
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
    column_editing_map = {
        "deferred": {"Deferred 1": "Deferred"},
        "work_location": {
            "Work from home (in the same grounds or building as your home)": "Working from home",
            "Working from home (in the same grounds or building as your home)": "Working from home",
            "From home (in the same grounds or building as your home)": "Working from home",
            "Work somewhere else (not your home)": "Working somewhere else (not your home)",
            "Somewhere else (not at your home)": "Working somewhere else (not your home)",
            "Somewhere else (not your home)": "Working somewhere else (not your home)",
            "Both (working from home and working somewhere else)": "Both (from home and somewhere else)",
            "Both (work from home and work somewhere else)": "Both (from home and somewhere else)",
        },
        "times_outside_shopping_or_socialising_last_7_days": v2_times_value_map,
        "times_shopping_last_7_days": v2_times_value_map,
        "times_socialising_last_7_days": v2_times_value_map,
        "times_hour_or_longer_another_home_last_7_days": v2_times_value_map,
        "times_hour_or_longer_another_person_your_home_last_7_days": v2_times_value_map,
        "work_sector": {
            "Social Care": "Social care",
            "Transport (incl. storage or logistic)": "Transport (incl. storage, logistic)",
            "Transport (incl. storage and logistic)": "Transport (incl. storage, logistic)",
            "Transport (incl. storage and logistics)": "Transport (incl. storage, logistic)",
            "Retail Sector (incl. wholesale)": "Retail sector (incl. wholesale)",
            "Hospitality (e.g. hotel or restaurant or cafe)": "Hospitality (e.g. hotel, restaurant)",
            "Food Production and agriculture (incl. farming)": "Food production, agriculture, farming",
            "Food production and agriculture (incl. farming)": "Food production, agriculture, farming",
            "Personal Services (e.g. hairdressers or tattooists)": "Personal services (e.g. hairdressers)",
            "Information technology and communication": "Information technology and communication",
            "Financial services (incl. insurance)": "Financial services incl. insurance",
            "Financial Services (incl. insurance)": "Financial services incl. insurance",
            "Civil Service or Local Government": "Civil service or Local Government",
            "Arts or Entertainment or Recreation": "Arts,Entertainment or Recreation",
            "Art or entertainment or recreation": "Arts,Entertainment or Recreation",
            "Arts or entertainment or recreation": "Arts,Entertainment or Recreation",
            "Other employment sector (specify)": "Other occupation sector",
            "Other occupation sector (specify)": "Other occupation sector",
        },
        "work_health_care_area": {
            "Primary Care (e.g. GP or dentist)": "Primary",
            "Primary care (e.g. GP or dentist)": "Primary",
            "Secondary Care (e.g. hospital)": "Secondary",
            "Secondary care (e.g. hospital.)": "Secondary",
            "Secondary care (e.g. hospital)": "Secondary",
            "Other Healthcare (e.g. mental health)": "Other",
            "Other healthcare (e.g. mental health)": "Other",
            "Participant Would Not/Could Not Answer": None,
            "Primary care for example in a GP or dentist": "Primary",
        },
        "face_covering_outside_of_home": {
            "My face is already covered for other reasons (e.g. religious or cultural reasons)": "My face is already covered",
            "Yes at work/school only": "Yes, at work/school only",
            "Yes in other situations only (including public transport/shops)": "Yes, in other situations only",
            "Yes usually both at work/school and in other situations": "Yes, usually both Work/school/other",
            "Yes in other situations only (including public transport or shops)": "Yes, in other situations only",
            "Yes always": "Yes, always",
            "Yes sometimes": "Yes, sometimes",
        },
        "face_covering_other_enclosed_places": {
            "My face is already covered for other reasons (e.g. religious or cultural reasons)": "My face is already covered",
            "Yes at work/school only": "Yes, at work/school only",
            "Yes in other situations only (including public transport/shops)": "Yes, in other situations only",
            "Yes usually both at work/school and in other situations": "Yes, usually both Work/school/other",
            "Yes always": "Yes, always",
            "Yes sometimes": "Yes, sometimes",
        },
        "face_covering_work_or_education": {
            "My face is already covered for other reasons (e.g. religious or cultural reasons)": "My face is already covered",
            "Yes always": "Yes, always",
            "Yes sometimes": "Yes, sometimes",
        },
        "other_antibody_test_results": {
            "One or more negative tests but none positive": "Any tests negative, but none positive",
            "One or more negative tests but none were positive": "Any tests negative, but none positive",
            "All tests failed": "All Tests failed",
        },
        "other_antibody_test_location": {
            "Private Lab": "Private lab",
            "Home Test": "Home test",
            "In the NHS (e.g. GP or hospital)": "In the NHS (e.g. GP, hospital)",
        },
        "other_covid_infection_test_results": {
            "One or more negative tests but none positive": "Any tests negative, but none positive",
            "One or more negative tests but none were positive": "Any tests negative, but none positive",
            "All tests failed": "All Tests failed",
            "Positive": "One or more positive test(s)",
            "Negative": "Any tests negative, but none positive",
            "Void": "All Tests failed",
        },
        "illness_reduces_activity_or_ability": {
            "Yes a little": "Yes, a little",
            "Yes a lot": "Yes, a lot",
            "Participant Would Not/Could Not Answer": None,
        },
        "participant_visit_status": {"Participant did not attend": "Patient did not attend", "Canceled": "Cancelled"},
        "self_isolating_reason": {
            "Yes for other reasons (e.g. going into hospital or quarantining)": "Yes, for other reasons (e.g. going into hospital, quarantining)",
            "Yes for other reasons related to reducing your risk of getting COVID-19 (e.g. going into hospital or shielding)": "Yes, for other reasons (e.g. going into hospital, quarantining)",
            "Yes for other reasons related to you having had an increased risk of getting COVID-19 (e.g. having been in contact with a known case or quarantining after travel abroad)": "Yes, for other reasons (e.g. going into hospital, quarantining)",
            "Yes because you live with someone who has/has had symptoms but you haven’t had them yourself": "Yes, someone you live with had symptoms",
            "Yes because you live with someone who has/has had symptoms or a positive test but you haven’t had symptoms yourself": "Yes, someone you live with had symptoms",
            "Yes because you live with someone who has/has had symptoms but you haven't had them yourself": "Yes, someone you live with had symptoms",
            "Yes because you have/have had symptoms of COVID-19": "Yes, you have/have had symptoms",
            "Yes because you have/have had symptoms of COVID-19 or a positive test": "Yes, you have/have had symptoms",
        },
        "ability_to_socially_distance_at_work_or_education": {
            "Difficult to maintain 2 meters - but I can usually be at least 1m from other people": "Difficult to maintain 2m, but can be 1m",
            "Difficult to maintain 2m - but you can usually be at least 1m from other people": "Difficult to maintain 2m, but can be 1m",
            "Easy to maintain 2 meters - it is not a problem to stay this far away from other people": "Easy to maintain 2m",
            "Easy to maintain 2m - it is not a problem to stay this far away from other people": "Easy to maintain 2m",
            "Relatively easy to maintain 2 meters - most of the time I can be 2m away from other people": "Relatively easy to maintain 2m",
            "Relatively easy to maintain 2m - most of the time you can be 2m away from other people": "Relatively easy to maintain 2m",
            "Very difficult to be more than 1 meter away as my work means I am in close contact with others on a regular basis": "Very difficult to be more than 1m away",
            "Very difficult to be more than 1m away as your work means you are in close contact with others on a regular basis": "Very difficult to be more than 1m away",
        },
        "transport_to_work_or_education": {
            "Bus or Minibus or Coach": "Bus, minibus, coach",
            "Bus or minibus or coach": "Bus, minibus, coach",
            "Bus": "Bus, minibus, coach",
            "Motorbike or Scooter or Moped": "Motorbike, scooter or moped",
            "Motorbike or scooter or moped": "Motorbike, scooter or moped",
            "Car or Van": "Car or van",
            "Taxi/Minicab": "Taxi/minicab",
            "On Foot": "On foot",
            "Underground or Metro or Light Rail or Tram": "Underground, metro, light rail, tram",
            "Other Method": "Other method",
        },
        "last_covid_contact_type": {
            "In your own household": "Living in your own home",
            "Outside your household": "Outside your home",
        },
        "last_suspected_covid_contact_type": {
            "In your own household": "Living in your own home",
            "Outside your household": "Outside your home",
        },
    }
    df = apply_value_map_multiple_columns(df, column_editing_map)
    df = df.withColumn("deferred", F.when(F.col("deferred").isNull(), "NA").otherwise(F.col("deferred")))

    df = df.withColumn("swab_sample_barcode", F.upper(F.col("swab_sample_barcode")))
    df = df.withColumn("blood_sample_barcode", F.upper(F.col("blood_sample_barcode")))
    return df


def derive_work_status_columns(df: DataFrame) -> DataFrame:
    work_status_dict = {
        "work_status_v0": {
            "5y and older in full-time education": "Student",
            "Attending college or other further education provider (including apprenticeships) (including if temporarily absent)": "Student",
            # noqa: E501
            "Employed and currently not working (e.g. on leave due to the COVID-19 pandemic (furloughed) or sick leave for 4 weeks or longer or maternity/paternity leave)": "Furloughed (temporarily not working)",
            # noqa: E501
            "Self-employed and currently not working (e.g. on leave due to the COVID-19 pandemic (furloughed) or sick leave for 4 weeks or longer or maternity/paternity leave)": "Furloughed (temporarily not working)",
            # noqa: E501
            "Self-employed and currently working (include if on leave or sick leave for less than 4 weeks)": "Self-employed",
            # noqa: E501
            "Employed and currently working (including if on leave or sick leave for less than 4 weeks)": "Employed",
            # noqa: E501
            "4-5y and older at school/home-school (including if temporarily absent)": "Student",  # noqa: E501
            "Not in paid work and not looking for paid work (include doing voluntary work here)": "Not working (unemployed, retired, long-term sick etc.)",
            # noqa: E501
            "Not working and not looking for work (including voluntary work)": "Not working (unemployed, retired, long-term sick etc.)",
            "Retired (include doing voluntary work here)": "Not working (unemployed, retired, long-term sick etc.)",
            # noqa: E501
            "Looking for paid work and able to start": "Not working (unemployed, retired, long-term sick etc.)",
            # noqa: E501
            "Child under 4-5y not attending nursery or pre-school or childminder": "Student",  # noqa: E501
            "Self-employed and currently not working (e.g. on leave due to the COVID-19 pandemic or sick leave for 4 weeks or longer or maternity/paternity leave)": "Furloughed (temporarily not working)",
            # noqa: E501
            "Child under 5y attending nursery or pre-school or childminder": "Student",  # noqa: E501
            "Child under 4-5y attending nursery or pre-school or childminder": "Student",  # noqa: E501
            "Retired": "Not working (unemployed, retired, long-term sick etc.)",  # noqa: E501
            "Attending university (including if temporarily absent)": "Student",  # noqa: E501
            "Not working and not looking for work": "Not working (unemployed, retired, long-term sick etc.)",
            # noqa: E501
            "Child under 5y not attending nursery or pre-school or childminder": "Student",  # noqa: E501
        },
        "work_status_v1": {
            "Child under 5y attending child care": "Child under 5y attending child care",  # noqa: E501
            "Child under 5y attending nursery or pre-school or childminder": "Child under 5y attending child care",
            # noqa: E501
            "Child under 4-5y attending nursery or pre-school or childminder": "Child under 5y attending child care",
            # noqa: E501
            "Child under 5y not attending nursery or pre-school or childminder": "Child under 5y not attending child care",
            # noqa: E501
            "Child under 5y not attending child care": "Child under 5y not attending child care",  # noqa: E501
            "Child under 4-5y not attending nursery or pre-school or childminder": "Child under 5y not attending child care",
            # noqa: E501
            "Employed and currently not working (e.g. on leave due to the COVID-19 pandemic (furloughed) or sick leave for 4 weeks or longer or maternity/paternity leave)": "Employed and currently not working",
            # noqa: E501
            "Employed and currently working (including if on leave or sick leave for less than 4 weeks)": "Employed and currently working",
            # noqa: E501
            "Not working and not looking for work (including voluntary work)": "Not working and not looking for work",
            # noqa: E501
            "Not in paid work and not looking for paid work (include doing voluntary work here)": "Not working and not looking for work",
            "Not working and not looking for work": "Not working and not looking for work",  # noqa: E501
            "Self-employed and currently not working (e.g. on leave due to the COVID-19 pandemic (furloughed) or sick leave for 4 weeks or longer or maternity/paternity leave)": "Self-employed and currently not working",
            # noqa: E501
            "Self-employed and currently not working (e.g. on leave due to the COVID-19 pandemic or sick leave for 4 weeks or longer or maternity/paternity leave)": "Self-employed and currently not working",
            # noqa: E501
            "Self-employed and currently working (include if on leave or sick leave for less than 4 weeks)": "Self-employed and currently working",
            # noqa: E501
            "Retired (include doing voluntary work here)": "Retired",  # noqa: E501
            "Looking for paid work and able to start": "Looking for paid work and able to start",  # noqa: E501
            "Attending college or other further education provider (including apprenticeships) (including if temporarily absent)": "5y and older in full-time education",
            # noqa: E501
            "Attending university (including if temporarily absent)": "5y and older in full-time education",
            # noqa: E501
            "4-5y and older at school/home-school (including if temporarily absent)": "5y and older in full-time education",
            # noqa: E501
        },
        "work_status_v2": {
            "Retired (include doing voluntary work here)": "Retired",  # noqa: E501
            "Attending college or other further education provider (including apprenticeships) (including if temporarily absent)": "Attending college or FE (including if temporarily absent)",
            # noqa: E501
            "Attending university (including if temporarily absent)": "Attending university (including if temporarily absent)",
            # noqa: E501
            "Child under 5y attending child care": "Child under 4-5y attending child care",  # noqa: E501
            "Child under 5y attending nursery or pre-school or childminder": "Child under 4-5y attending child care",
            # noqa: E501
            "Child under 4-5y attending nursery or pre-school or childminder": "Child under 4-5y attending child care",
            # noqa: E501
            "Child under 5y not attending nursery or pre-school or childminder": "Child under 4-5y not attending child care",
            # noqa: E501
            "Child under 5y not attending child care": "Child under 4-5y not attending child care",  # noqa: E501
            "Child under 4-5y not attending nursery or pre-school or childminder": "Child under 4-5y not attending child care",
            # noqa: E501
            "4-5y and older at school/home-school (including if temporarily absent)": "4-5y and older at school/home-school",
            # noqa: E501
            "Employed and currently not working (e.g. on leave due to the COVID-19 pandemic (furloughed) or sick leave for 4 weeks or longer or maternity/paternity leave)": "Employed and currently not working",
            # noqa: E501
            "Employed and currently working (including if on leave or sick leave for less than 4 weeks)": "Employed and currently working",
            # noqa: E501
            "Not in paid work and not looking for paid work (include doing voluntary work here)": "Not working and not looking for work",
            # noqa: E501
            "Not working and not looking for work (including voluntary work)": "Not working and not looking for work",
            # noqa: E501
            "Self-employed and currently not working (e.g. on leave due to the COVID-19 pandemic or sick leave for 4 weeks or longer or maternity/paternity leave)": "Self-employed and currently not working",
            # noqa: E501
            "Self-employed and currently not working (e.g. on leave due to the COVID-19 pandemic (furloughed) or sick leave for 4 weeks or longer or maternity/paternity leave)": "Self-employed and currently not working",
            # noqa: E501
            "Self-employed and currently working (include if on leave or sick leave for less than 4 weeks)": "Self-employed and currently working",
            # noqa: E501
            "5y and older in full-time education": "4-5y and older at school/home-school",  # noqa: E501
        },
    }

    column_list = ["work_status_v0", "work_status_v1"]
    for column in column_list:
        df = df.withColumn(column, F.col("work_status_v2"))
        df = update_column_values_from_map(df=df, column=column, map=work_status_dict[column])

    df = update_column_values_from_map(df=df, column="work_status_v2", map=work_status_dict["work_status_v2"])
    return df


def transform_survey_responses_version_2_delta(df: DataFrame) -> DataFrame:
    """
    Transformations that are specific to version 2 survey responses.
    """
    raw_copy_list = ["cis_covid_vaccine_number_of_doses"]

    df = assign_raw_copies(df, [column for column in raw_copy_list if column in df.columns])

    df = assign_taken_column(df=df, column_name_to_assign="swab_taken", reference_column="swab_sample_barcode")
    df = assign_taken_column(df=df, column_name_to_assign="blood_taken", reference_column="blood_sample_barcode")

    df = assign_column_uniform_value(df, "survey_response_dataset_major_version", 2)

    # After editing to V1 values in cleaning
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
    df = edit_to_sum_or_max_value(
        df=df,
        column_name_to_assign="times_outside_shopping_or_socialising_last_7_days",
        columns_to_sum=[
            "times_shopping_last_7_days",
            "times_socialising_last_7_days",
        ],
        max_value=7,
    )
    df = derive_work_status_columns(df)
    return df


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


def translate_welsh_survey_responses_version_digital(df: DataFrame) -> DataFrame:
    """
    High-level function that manages the translation of both fixed-text and free-text Welsh responses in the
    CIS Digital response extract.

    Loads values from config to determine if the translation_project_workflow_enabled flag is true, if the
    translation_project_workflow_enabled flag is true then it works through the translation project workflow to:
    1. check for new completed free-text translations
    2. if there are new completed free-text translations then backup and update the translations_lookup_df
    3. update the df from the translations_lookup_df
    4. filter the df to get free-text responses to be translated
    5. export free-text responses to be translated

    If the translation_project_workflow_enabled flag is false then it will just carry out step 3. above.
    After this check it will update the fixed-text responses in the df.

    This function requires that the config contains a valid translation_lookup_path in the storage dictionary of
    the pipeline_config file.

    Parameters
    ----------
    df :  DataFrame
        df containing free- or fixed-text responses to translate)

    Returns
    -------
    df : DataFrame
        df incorporating any available translations to free- or fixed-text responses
    """
    translation_settings = get_config().get("translation", {"inactive": "inactive"})
    storage_settings = get_config().get("storage", {"inactive": "inactive"})
    translation_lookup_path = storage_settings.get("translation_lookup_path", "inactive")

    translation_settings_in_pipeline_config = translation_settings != {"inactive": "inactive"}
    translation_lookup_path_in_pipeline_config = translation_lookup_path != "inactive"

    if translation_settings_in_pipeline_config:

        translation_directory = translation_settings.get("translation_directory", None)
        translation_lookup_directory = translation_settings.get("translation_lookup_directory", None)
        translation_backup_directory = translation_settings.get("translation_backup_directory", None)

        new_translations_df = get_new_translations_from_completed_translations_directory(
            translation_directory=translation_directory,
            translation_lookup_path=translation_lookup_path,
        )

        if new_translations_df.count() > 0:
            translation_lookup_df = extract_lookup_csv(
                translation_lookup_path, validation_schemas["csv_lookup_schema_extended"]
            )

            new_translations_lookup_df = translation_lookup_df.union(new_translations_df)

            backup_and_replace_translation_lookup_df(
                new_lookup_df=new_translations_lookup_df,
                lookup_directory=translation_lookup_directory,
                backup_directory=translation_backup_directory,
            )

        df = translate_welsh_free_text_responses_digital(
            df=df,
            lookup_path=translation_lookup_path,
        )

        to_be_translated_df = get_welsh_responses_to_be_translated(df)

        if to_be_translated_df.count() > 0:
            export_responses_to_be_translated_to_translation_directory(
                to_be_translated_df=to_be_translated_df, translation_directory=translation_directory
            )

    if translation_lookup_path_in_pipeline_config and not translation_settings_in_pipeline_config:
        df = translate_welsh_free_text_responses_digital(
            df=df,
            lookup_path=translation_lookup_path,
        )

    df = translate_welsh_fixed_text_responses_digital(df)

    return df


def digital_responses_preprocessing(df: DataFrame) -> DataFrame:
    """
    Call transformations to digital data necessary before generic transformations are applied
    """
    df = assign_column_uniform_value(df, "survey_response_dataset_major_version", 3)
    df = assign_date_from_filename(df, "file_date", "survey_response_source_file")
    df = update_strings_to_sentence_case(df, ["survey_completion_status", "survey_not_completed_reason_code"])
    df = df.withColumn("visit_id", F.col("participant_completion_window_id"))
    df = df.withColumn(
        "swab_manual_entry", F.when(F.col("swab_sample_barcode_user_entered").isNull(), "No").otherwise("Yes")
    )
    df = df.withColumn(
        "blood_manual_entry", F.when(F.col("blood_sample_barcode_user_entered").isNull(), "No").otherwise("Yes")
    )

    df = assign_datetime_from_coalesced_columns_and_log_source(
        df,
        column_name_to_assign="visit_datetime",
        source_reference_column_name="visit_date_type",
        primary_datetime_columns=[
            "swab_taken_datetime",
            "blood_taken_datetime",
            "survey_completed_datetime",
            "survey_last_modified_datetime",
        ],
        secondary_date_columns=[],
        min_datetime_column_name="participant_completion_window_start_datetime",
        max_datetime_column_name="participant_completion_window_end_datetime",
        reference_datetime_column_name="swab_sample_received_consolidation_point_datetime",
        default_timestamp="12:00:00",
        final_fallback_column="participant_completion_window_start_datetime",
    )
    df = update_column_in_time_window(
        df,
        "digital_survey_collection_mode",
        "survey_completed_datetime",
        "Telephone",
        ["20-05-2022T21:30:00", "25-05-2022 11:00:00"],
    )
    return df


def transform_survey_responses_version_digital_delta(df: DataFrame) -> DataFrame:
    """
    Call functions to process digital specific variable transformations.
    """
    df = assign_column_value_from_multiple_column_map(
        df,
        "self_isolating_reason",
        [
            [
                "No",
                ["No", None],
            ],
            [
                "Yes, you have/have had symptoms",
                ["Yes", "I have or have had symptoms of COVID-19 or a positive test"],
            ],
            [
                "Yes, someone you live with had symptoms",
                [
                    "Yes",
                    [
                        "I haven't had any symptoms but I live with someone who has or has had symptoms or a positive test",
                        # noqa: E501
                        # TODO: Remove once encoding fixed in raw data
                        "I haven&#39;t had any symptoms but I live with someone who has or has had symptoms or a positive test",
                        # noqa: E501
                    ],
                ],
            ],
            [
                "Yes, for other reasons (e.g. going into hospital, quarantining)",
                [
                    "Yes",
                    "Due to increased risk of getting COVID-19 such as having been in contact with a known case or quarantining after travel abroad",
                ],  # noqa: E501
            ],
            [
                "Yes, for other reasons (e.g. going into hospital, quarantining)",
                [
                    "Yes",
                    "Due to reducing my risk of getting COVID-19 such as going into hospital or shielding",
                ],  # noqa: E501
            ],
        ],
        ["self_isolating", "self_isolating_reason_detailed"],
    )

    column_list = ["work_status_digital", "work_status_employment", "work_status_unemployment", "work_status_education"]
    df = assign_column_value_from_multiple_column_map(
        df,
        "work_status_v2",
        [
            [
                "Employed and currently working",
                [
                    "Employed",
                    "Currently working. This includes if you are on sick or other leave for less than 4 weeks",
                    None,
                    None,
                ],
            ],
            [
                "Employed and currently not working",
                [
                    "Employed",
                    [
                        "Currently not working -  for example on sick or other leave such as maternity or paternity for longer than 4 weeks",
                        # noqa: E501
                        "Or currently not working -  for example on sick or other leave such as maternity or paternity for longer than 4 weeks?",
                        # noqa: E501
                    ],
                    None,
                    None,
                ],
            ],
            [
                "Self-employed and currently working",
                [
                    "Self-employed",
                    "Currently working. This includes if you are on sick or other leave for less than 4 weeks",
                    None,
                    None,
                ],
            ],
            [
                "Self-employed and currently not working",
                [
                    "Self-employed",
                    [
                        "Currently not working -  for example on sick or other leave such as maternity or paternity for longer than 4 weeks",
                        # noqa: E501
                        "Or currently not working -  for example on sick or other leave such as maternity or paternity for longer than 4 weeks?",
                        # noqa: E501
                    ],
                    None,
                    None,
                ],
            ],
            [
                "Looking for paid work and able to start",
                [
                    "Not in paid work. This includes being unemployed or retired or doing voluntary work",
                    None,
                    "Looking for paid work and able to start",
                    None,
                ],
            ],
            [
                "Not working and not looking for work",
                [
                    "Not in paid work. This includes being unemployed or retired or doing voluntary work",
                    None,
                    "Not looking for paid work. This includes looking after the home or family or not wanting a job or being long-term sick or disabled",
                    # noqa: E501
                    None,
                ],
            ],
            [
                "Retired",
                [
                    "Not in paid work. This includes being unemployed or retired or doing voluntary work",
                    None,
                    ["Retired", "Or retired?"],
                    None,
                ],
            ],
            [
                "Child under 4-5y not attending child care",
                [
                    ["In education", None],
                    None,
                    None,
                    "A child below school age and not attending a nursery or pre-school or childminder",
                ],
            ],
            [
                "Child under 4-5y attending child care",
                [
                    ["In education", None],
                    None,
                    None,
                    "A child below school age and attending a nursery or a pre-school or childminder",
                ],
            ],
            [
                "4-5y and older at school/home-school",
                [
                    ["In education", None],
                    None,
                    None,
                    ["A child aged 4 or over at school", "A child aged 4 or over at home-school"],
                ],
            ],
            [
                "Attending college or FE (including if temporarily absent)",
                [
                    ["In education", None],
                    None,
                    None,
                    "Attending a college or other further education provider including apprenticeships",
                ],
            ],
            [
                "Attending university (including if temporarily absent)",
                [
                    ["In education", None],
                    None,
                    None,
                    ["Attending university", "Or attending university?"],
                ],
            ],
        ],
        column_list,
    )
    df = assign_column_value_from_multiple_column_map(
        df,
        "work_status_v1",
        [
            [
                "Employed and currently working",
                [
                    "Employed",
                    "Currently working. This includes if you are on sick or other leave for less than 4 weeks",
                    None,
                    None,
                ],
            ],
            [
                "Employed and currently not working",
                [
                    "Employed",
                    "Currently not working -  for example on sick or other leave such as maternity or paternity for longer than 4 weeks",
                    # noqa: E501
                    None,
                    None,
                ],
            ],
            [
                "Self-employed and currently working",
                [
                    "Self-employed",
                    "Currently working. This includes if you are on sick or other leave for less than 4 weeks",
                    None,
                    None,
                ],
            ],
            [
                "Self-employed and currently not working",
                [
                    "Self-employed",
                    "Currently not working -  for example on sick or other leave such as maternity or paternity for longer than 4 weeks",
                    None,
                    None,
                ],
            ],
            [
                "Looking for paid work and able to start",
                [
                    "Not in paid work. This includes being unemployed or retired or doing voluntary work",
                    None,
                    "Looking for paid work and able to start",
                    None,
                ],
            ],
            [
                "Not working and not looking for work",
                [
                    "Not in paid work. This includes being unemployed or retired or doing voluntary work",
                    None,
                    "Not looking for paid work. This includes looking after the home or family or not wanting a job or being long-term sick or disabled",
                    None,
                ],
            ],
            [
                "Retired",
                [
                    "Not in paid work. This includes being unemployed or retired or doing voluntary work",
                    None,
                    ["Or retired?", "Retired"],
                    None,
                ],
            ],
            [
                "Child under 5y not attending child care",
                [
                    ["In education", None],
                    None,
                    None,
                    "A child below school age and not attending a nursery or pre-school or childminder",
                ],
            ],
            [
                "Child under 5y attending child care",
                [
                    ["In education", None],
                    None,
                    None,
                    "A child below school age and attending a nursery or a pre-school or childminder",
                ],
            ],
            [
                "5y and older in full-time education",
                [
                    ["In education", None],
                    None,
                    None,
                    [
                        "A child aged 4 or over at school",
                        "A child aged 4 or over at home-school",
                        "Attending a college or other further education provider including apprenticeships",
                        "Attending university",
                    ],
                ],
            ],
        ],
        column_list,
    )
    df = assign_column_value_from_multiple_column_map(
        df,
        "work_status_v0",
        [
            [
                "Employed",
                [
                    "Employed",
                    "Currently working. This includes if you are on sick or other leave for less than 4 weeks",
                    None,
                    None,
                ],
            ],
            [
                "Not working (unemployed, retired, long-term sick etc.)",
                [
                    "Employed",
                    "Currently not working -  for example on sick or other leave such as maternity or paternity for longer than 4 weeks",
                    # noqa: E501
                    None,
                    None,
                ],
            ],
            ["Employed", ["Employed", None, None, None]],
            [
                "Self-employed",
                [
                    "Self-employed",
                    "Currently working. This includes if you are on sick or other leave for less than 4 weeks",
                    None,
                    None,
                ],
            ],
            ["Self-employed", ["Self-employed", None, None, None]],
            [
                "Not working (unemployed, retired, long-term sick etc.)",
                [
                    "Self-employed",
                    "Currently not working -  for example on sick or other leave such as maternity or paternity for longer than 4 weeks",
                    # noqa: E501,
                    None,
                    None,
                ],
            ],
            [
                "Not working (unemployed, retired, long-term sick etc.)",
                [
                    "Not in paid work. This includes being unemployed or retired or doing voluntary work",
                    None,
                    "Looking for paid work and able to start",
                    None,
                ],
            ],
            [
                "Not working (unemployed, retired, long-term sick etc.)",
                [
                    "Not in paid work. This includes being unemployed or retired or doing voluntary work",
                    None,
                    "Not looking for paid work. This includes looking after the home or family or not wanting a job or being long-term sick or disabled",
                    # noqa: E501
                    None,
                ],
            ],
            [
                "Not working (unemployed, retired, long-term sick etc.)",
                [
                    "Not in paid work. This includes being unemployed or retired or doing voluntary work",
                    None,
                    ["Retired", "Or retired?"],
                    None,
                ],
            ],
            [
                "Not working (unemployed, retired, long-term sick etc.)",
                [
                    "Not in paid work. This includes being unemployed or retired or doing voluntary work",
                    None,
                    None,
                    None,
                ],
            ],
            [
                "Student",
                [
                    ["In education", None],
                    None,
                    None,
                    [
                        "A child below school age and not attending a nursery or pre-school or childminder",
                        "A child below school age and attending a nursery or a pre-school or childminder",
                        "A child aged 4 or over at school",
                        "A child aged 4 or over at home-school",
                        "Attending a college or other further education provider including apprenticeships",
                        "Attending university",
                    ],
                ],
            ],
            ["Student", ["In education", None, None, None]],
        ],
        column_list,
    )
    df = clean_barcode_simple(df, "swab_sample_barcode_user_entered")
    df = clean_barcode_simple(df, "blood_sample_barcode_user_entered")

    df = clean_barcode_simple(df, "swab_barcode_corrected")
    df = clean_barcode_simple(df, "blood_barcode_corrected")

    df = map_options_to_bool_columns(
        df,
        "currently_smokes_or_vapes_description",
        {
            "Cigarettes": "smoke_cigarettes",
            "Cigars": "smokes_cigar",
            "Pipe": "smokes_pipe",
            "Vape or E-cigarettes": "smokes_vape_e_cigarettes",
            "Hookah or shisha pipes": "smokes_hookah_shisha_pipes",
        },
        ";",
    )
    df = map_options_to_bool_columns(
        df,
        "blood_not_taken_missing_parts",
        {
            "Small sample test tube. This is the tube that is used to collect the blood.": "blood_not_taken_missing_parts_small_sample_tube",
            # noqa: E501
            "Large sample carrier tube with barcode on. This is the tube that you put the small sample test tube in to after collecting blood.": "blood_not_taken_missing_parts_large_sample_carrier",
            # noqa: E501
            "Re-sealable biohazard bag with absorbent pad": "blood_not_taken_missing_parts_biohazard_bag",
            "Copy of your blood barcode": "blood_not_taken_missing_parts_blood_barcode",
            "Lancets": "blood_not_taken_missing_parts_lancets",
            "Plasters": "blood_not_taken_missing_parts_plasters",
            "Alcohol wipes": "blood_not_taken_missing_parts_alcohol_wipes",
            "Cleansing wipe": "blood_not_taken_missing_parts_cleansing_wipe",
            "Sample box": "blood_not_taken_missing_parts_sample_box",
            "Sample return bag with a return label on": "blood_not_taken_missing_parts_sample_return_bag",
            "Other please specify": "blood_not_taken_missing_parts_other",
        },
        ";",
    )
    df = map_options_to_bool_columns(
        df,
        "blood_not_taken_could_not_reason",
        {
            "I couldn't get enough blood into the pot": "blood_not_taken_could_not_reason_not_enough_blood",
            "The pot spilled": "blood_not_taken_could_not_reason_pot_spilled",
            "I had bruising or pain": "blood_not_taken_could_not_reason_had_bruising",
            "I felt unwell": "blood_not_taken_could_not_reason_unwell",
            "Other please specify": "blood_not_taken_could_not_reason_other",
        },
        ";",
    )
    df = map_options_to_bool_columns(
        df,
        "swab_not_taken_missing_parts",
        {
            "Sample pot with fluid in the bottom and barcode on": "swab_not_taken_missing_parts_sample_pot",
            "Swab stick": "swab_not_taken_missing_parts_swab_stick",
            "Re-sealable biohazard bag with absorbent pad": "swab_not_taken_missing_parts_biohazard_bag",
            "Copy of your swab barcode": "swab_not_taken_missing_parts_swab_barcode",
            "Sample box": "swab_not_taken_missing_parts_sample_box",
            "Sample return bag with a return label on": "swab_not_taken_missing_parts_return_bag",
            "Other please specify": "swab_not_taken_missing_parts_other",
        },
        ";",
    )
    df = df.withColumn("times_outside_shopping_or_socialising_last_7_days", F.lit(None))
    raw_copy_list = [
        "participant_survey_status",
        "participant_withdrawal_type",
        "survey_response_type",
        "work_sector",
        "illness_reduces_activity_or_ability",
        "ability_to_socially_distance_at_work_or_education",
        "last_covid_contact_type",
        "last_suspected_covid_contact_type",
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
        "other_covid_infection_test_results",
        "other_antibody_test_results",
        "cis_covid_vaccine_type",
        "cis_covid_vaccine_type_other",
        "cis_covid_vaccine_number_of_doses",
        "cis_covid_vaccine_type_1",
        "cis_covid_vaccine_type_2",
        "cis_covid_vaccine_type_3",
        "cis_covid_vaccine_type_4",
        "cis_covid_vaccine_type_5",
        "cis_covid_vaccine_type_6",
        "cis_covid_vaccine_type_other_1",
        "cis_covid_vaccine_type_other_2",
        "cis_covid_vaccine_type_other_3",
        "cis_covid_vaccine_type_other_4",
        "cis_covid_vaccine_type_other_5",
        "cis_covid_vaccine_type_other_6",
    ]
    df = assign_raw_copies(df, [column for column in raw_copy_list if column in df.columns])
    """
    Sets categories to map for digital specific variables to Voyager 0/1/2 equivalent
    """
    contact_people_value_map = {
        "1 to 5": "1-5",
        "6 to 10": "6-10",
        "11 to 20": "11-20",
        "Don't know": None,
        "Prefer not to say": None,
    }
    times_value_map = {
        "1": 1,
        "2": 2,
        "3": 3,
        "4": 4,
        "5": 5,
        "6": 6,
        "7 times or more": 7,
        "Don't know": None,
        "None": 0,
        "Prefer not to say": None,
    }
    vaccine_type_map = {
        "Pfizer / BioNTech": "Pfizer/BioNTech",
        "Oxford / AstraZeneca": "Oxford/AstraZeneca",
        "Janssen / Johnson&Johnson": "Janssen/Johnson&Johnson",
        "Another vaccine please specify": "Other / specify",
        "I don't know the type": "Don't know type",
        "Or Another vaccine please specify": "Other / specify",  # changed from "Other /specify"
        "I do not know the type": "Don't know type",
        "Or do you not know which one you had?": "Don't know type",
        "I don&#39;t know the type": "Don't know type",
        "I don&amp;#39;t know the type": "Don't know type",
        "I dont know the type": "Don't know type",
        "Janssen / Johnson&amp;Johnson": "Janssen/Johnson&Johnson",
        "Janssen / Johnson&amp;amp;Johnson": "Janssen/Johnson&Johnson",
        "Oxford also known as AstraZeneca": "Oxford/AstraZeneca",
        "Pfizer also known as BioNTech": "Pfizer/BioNTech",
    }
    column_editing_map = {
        "participant_survey_status": {"Complete": "Completed"},
        "participant_withdrawal_type": {
            "Withdrawn - no future linkage": "Withdrawn_no_future_linkage",
            "Withdrawn - no future linkage or use of samples": "Withdrawn_no_future_linkage_or_use_of_samples",
        },
        "survey_response_type": {"First Survey": "First Visit", "Follow-up Survey": "Follow-up Visit"},
        "voucher_type_preference": {"Letter": "Paper", "Email": "email_address"},
        "work_sector": {
            "Social Care": "Social care",
            "Transport. This includes storage and logistics": "Transport (incl. storage, logistic)",
            "Retail sector. This includes wholesale": "Retail sector (incl. wholesale)",
            "Hospitality - for example hotels or restaurants or cafe": "Hospitality (e.g. hotel, restaurant)",
            "Food production and agriculture. This includes farming": "Food production, agriculture, farming",
            "Personal Services - for example hairdressers or tattooists": "Personal services (e.g. hairdressers)",
            "Information technology and communication": "Information technology and communication",
            "Financial services. This includes insurance": "Financial services incl. insurance",
            "Civil Service or Local Government": "Civil service or Local Government",
            "Arts or entertainment or recreation": "Arts,Entertainment or Recreation",
            "Other employment sector please specify": "Other occupation sector",
        },
        "work_health_care_area": {
            "Secondary care for example in a hospital": "Secondary",
            "Another type of healthcare - for example mental health services?": "Other",
            "Primary care - for example in a GP or dentist": "Primary",
            "Yes, in primary care, e.g. GP, dentist": "Primary",
            "Secondary care - for example in a hospital": "Secondary",
            "Another type of healthcare - for example mental health services": "Other",  # noqa: E501
        },
        "illness_reduces_activity_or_ability": {
            "Yes a little": "Yes, a little",
            "Yes a lot": "Yes, a lot",
        },
        "work_location": {
            "From home meaning in the same grounds or building as your home": "Working from home",
            "Somewhere else meaning not at your home": "Working somewhere else (not your home)",
            "Both from home and work somewhere else": "Both (from home and somewhere else)",
        },
        "transport_to_work_or_education": {
            "Bus or minibus or coach": "Bus, minibus, coach",
            "Motorbike or scooter or moped": "Motorbike, scooter or moped",
            "Taxi or minicab": "Taxi/minicab",
            "Underground or Metro or Light Rail or Tram": "Underground, metro, light rail, tram",
        },
        "ability_to_socially_distance_at_work_or_education": {
            "Difficult to maintain 2 metres apart. But you can usually be at least 1 metre away from other people": "Difficult to maintain 2m, but can be 1m",
            # noqa: E501
            "Easy to maintain 2 metres apart. It is not a problem to stay this far away from other people": "Easy to maintain 2m",
            # noqa: E501
            "Relatively easy to maintain 2 metres apart. Most of the time you can be 2 meters away from other people": "Relatively easy to maintain 2m",
            # noqa: E501
            "Very difficult to be more than 1 metre away. Your work means you are in close contact with others on a regular basis": "Very difficult to be more than 1m away",
        },
        "last_covid_contact_type": {
            "Someone I live with": "Living in your own home",
            "Someone I do not live with": "Outside your home",
        },
        "last_suspected_covid_contact_type": {
            "Someone I live with": "Living in your own home",
            "Someone I do not live with": "Outside your home",
        },
        "physical_contact_under_18_years": contact_people_value_map,
        "physical_contact_18_to_69_years": contact_people_value_map,
        "physical_contact_over_70_years": contact_people_value_map,
        "social_distance_contact_under_18_years": contact_people_value_map,
        "social_distance_contact_18_to_69_years": contact_people_value_map,
        "social_distance_contact_over_70_years": contact_people_value_map,
        "times_hour_or_longer_another_home_last_7_days": times_value_map,
        "times_hour_or_longer_another_person_your_home_last_7_days": times_value_map,
        "times_shopping_last_7_days": times_value_map,
        "times_socialising_last_7_days": times_value_map,
        "face_covering_work_or_education": {
            "Prefer not to say": None,
            "Yes sometimes": "Yes, sometimes",
            "Yes always": "Yes, always",
            "I am not going to my place of work or education": "Not going to place of work or education",
            "I cover my face for other reasons - for example for religious or cultural reasons": "My face is already covered",
            # noqa: E501
        },
        "face_covering_other_enclosed_places": {
            "Prefer not to say": None,
            "Yes sometimes": "Yes, sometimes",
            "Yes always": "Yes, always",
            "I am not going to other enclosed public spaces or using public transport": "Not going to other enclosed public spaces or using public transport",
            # noqa: E501
            "I cover my face for other reasons - for example for religious or cultural reasons": "My face is already covered",
            # noqa: E501
        },
        "other_covid_infection_test_results": {
            "All tests failed": "All Tests failed",
            "One or more tests were negative and none were positive": "Any tests negative, but none positive",
            "One or more tests were positive": "One or more positive test(s)",
        },
        "other_antibody_test_results": {
            "All tests failed": "All Tests failed",
            "One or more tests were negative for antibodies and none were positive": "Any tests negative, but none positive",
            # noqa: E501
            "One or more tests were positive for antibodies": "One or more positive test(s)",
        },
        "cis_covid_vaccine_type": vaccine_type_map,
        "cis_covid_vaccine_number_of_doses": {
            "1 dose": "1",
            "2 doses": "2",
            "3 doses": "3 or more",
            "4 doses": "3 or more",
            "5 doses": "3 or more",
            "6 doses or more": "3 or more",
        },
        "cis_covid_vaccine_type_1": vaccine_type_map,
        "cis_covid_vaccine_type_2": vaccine_type_map,
        "cis_covid_vaccine_type_3": vaccine_type_map,
        "cis_covid_vaccine_type_4": vaccine_type_map,
        "cis_covid_vaccine_type_5": vaccine_type_map,
        "cis_covid_vaccine_type_6": vaccine_type_map,
    }
    df = apply_value_map_multiple_columns(df, column_editing_map)

    df = edit_to_sum_or_max_value(
        df=df,
        column_name_to_assign="times_outside_shopping_or_socialising_last_7_days",
        columns_to_sum=[
            "times_shopping_last_7_days",
            "times_socialising_last_7_days",
        ],
        max_value=7,
    )
    df = df.withColumn(
        "work_not_from_home_days_per_week",
        F.greatest("work_not_from_home_days_per_week", "education_in_person_days_per_week"),
    )

    df = df.withColumn("face_covering_outside_of_home", F.lit(None).cast("string"))
    df = concat_fields_if_true(df, "think_had_covid_which_symptoms", "think_had_covid_which_symptom_", "Yes", ";")
    df = concat_fields_if_true(df, "which_symptoms_last_7_days", "think_have_covid_symptom_", "Yes", ";")
    df = concat_fields_if_true(df, "long_covid_symptoms", "think_have_long_covid_symptom_", "Yes", ";")
    df = survey_edit_auto_complete(
        df,
        "survey_completion_status",
        "participant_completion_window_end_datetime",
        "face_covering_other_enclosed_places",
        "file_date",
    )
    df = update_column_values_from_map(
        df,
        "survey_completion_status",
        {
            "In progress": "Partially Completed",
            "Submitted": "Completed",
        },
    )

    df = derive_had_symptom_last_7days_from_digital(
        df,
        "think_have_covid_symptom_any",
        "think_have_covid_symptom_",
        [
            "fever",
            "muscle_ache",
            "fatigue",
            "sore_throat",
            "cough",
            "shortness_of_breath",
            "headache",
            "nausea_or_vomiting",
            "abdominal_pain",
            "diarrhoea",
            "loss_of_taste",
            "loss_of_smell",
        ],
    )

    df = update_value_if_multiple_and_ref_in_list(
        df,
        "swab_consolidation_point_error",
        ["sample_leaked", "sample_uncompleted"],
        "multiple errors sample discarded",
        "multiple errors sample retained",
        ",",
    )

    df = update_value_if_multiple_and_ref_in_list(
        df,
        "blood_consolidation_point_error",
        ["sample_leaked", "sample_uncompleted"],
        "multiple errors sample discarded",
        "multiple errors sample retained",
        ",",
    )
    return df
