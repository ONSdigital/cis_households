# flake8: noqa
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window

from cishouseholds.derive import assign_age_at_date
from cishouseholds.derive import assign_column_from_mapped_list_key
from cishouseholds.derive import assign_column_given_proportion
from cishouseholds.derive import assign_column_regex_match
from cishouseholds.derive import assign_column_to_date_string
from cishouseholds.derive import assign_column_uniform_value
from cishouseholds.derive import assign_consent_code
from cishouseholds.derive import assign_date_difference
from cishouseholds.derive import assign_ethnicity_white
from cishouseholds.derive import assign_ever_had_long_term_health_condition_or_disabled
from cishouseholds.derive import assign_fake_id
from cishouseholds.derive import assign_first_visit
from cishouseholds.derive import assign_grouped_variable_from_days_since
from cishouseholds.derive import assign_household_participant_count
from cishouseholds.derive import assign_household_under_2_count
from cishouseholds.derive import assign_isin_list
from cishouseholds.derive import assign_last_visit
from cishouseholds.derive import assign_named_buckets
from cishouseholds.derive import assign_outward_postcode
from cishouseholds.derive import assign_raw_copies
from cishouseholds.derive import assign_school_year_september_start
from cishouseholds.derive import assign_taken_column
from cishouseholds.derive import assign_true_if_any
from cishouseholds.derive import assign_unique_id_column
from cishouseholds.derive import assign_work_health_care
from cishouseholds.derive import assign_work_patient_facing_now
from cishouseholds.derive import assign_work_person_facing_now
from cishouseholds.derive import assign_work_social_column
from cishouseholds.derive import assign_work_status_group
from cishouseholds.derive import contact_known_or_suspected_covid_type
from cishouseholds.derive import count_value_occurrences_in_column_subset_row_wise
from cishouseholds.derive import derive_household_been_columns
from cishouseholds.edit import apply_value_map_multiple_columns
from cishouseholds.edit import clean_postcode
from cishouseholds.edit import clean_within_range
from cishouseholds.edit import convert_null_if_not_in_list
from cishouseholds.edit import edit_to_sum_or_max_value
from cishouseholds.edit import format_string_upper_and_clean
from cishouseholds.edit import map_column_values_to_null
from cishouseholds.edit import update_column_if_ref_in_list
from cishouseholds.edit import update_column_values_from_map
from cishouseholds.edit import update_face_covering_outside_of_home
from cishouseholds.edit import update_person_count_from_ages
from cishouseholds.edit import update_symptoms_last_7_days_any
from cishouseholds.edit import update_to_value_if_any_not_null
from cishouseholds.edit import update_work_facing_now_column
from cishouseholds.expressions import sum_within_row
from cishouseholds.impute import fill_backwards_overriding_not_nulls
from cishouseholds.impute import fill_backwards_work_status_v2
from cishouseholds.impute import fill_forward_from_last_change
from cishouseholds.impute import fill_forward_only_to_nulls
from cishouseholds.impute import fill_forward_only_to_nulls_in_dataset_based_on_column
from cishouseholds.impute import impute_by_ordered_fill_forward
from cishouseholds.impute import impute_latest_date_flag
from cishouseholds.impute import impute_outside_uk_columns
from cishouseholds.impute import impute_visit_datetime
from cishouseholds.pipeline.timestamp_map import cis_digital_datetime_map
from cishouseholds.validate_class import SparkValidate


def transform_survey_responses_generic(df: DataFrame) -> DataFrame:
    """
    Generic transformation steps to be applied to all survey response records.
    """
    raw_copy_list = [
        "think_had_covid_any_symptoms",
        "symptoms_last_7_days_any",
        "work_main_job_title",
        "work_main_job_role",
        "work_health_care_v0",
        "work_health_care_v1_v2",
        "work_status_v1",
        "work_status_v2",
        "work_social_care",
        "work_not_from_home_days_per_week",
        "work_location",
        "sex",
        "withdrawal_reason",
        "blood_sample_barcode",
        "swab_sample_barcode",
    ]
    df = assign_raw_copies(df, [column for column in raw_copy_list if column in df.columns])
    df = assign_unique_id_column(
        df, "unique_participant_response_id", concat_columns=["visit_id", "participant_id", "visit_datetime"]
    )
    df = assign_column_regex_match(
        df, "bad_email", reference_column="email", pattern=r"/^w+[+.w-]*@([w-]+.)*w+[w-]*.([a-z]{2,4}|d+)$/i"
    )
    df = clean_postcode(df, "postcode")
    df = assign_outward_postcode(df, "outward_postcode", reference_column="postcode")
    df = assign_consent_code(
        df, "consent_summary", reference_columns=["consent_16_visits", "consent_5_visits", "consent_1_visit"]
    )
    df = assign_taken_column(df, "swab_taken", reference_column="swab_sample_barcode")
    df = assign_taken_column(df, "blood_taken", reference_column="blood_sample_barcode")

    df = assign_date_difference(df, "days_since_think_had_covid", "think_had_covid_date", "visit_datetime")
    df = assign_grouped_variable_from_days_since(
        df=df,
        binary_reference_column="think_had_covid",
        days_since_reference_column="days_since_think_had_covid",
        column_name_to_assign="days_since_think_had_covid_group",
    )
    df = df.withColumn("hh_id", F.col("ons_household_id"))
    df = update_column_values_from_map(
        df,
        "work_not_from_home_days_per_week",
        {"NA": "99", "N/A (not working/in education etc)": "99", "up to 1": "0.5"},
    )
    return df


def derive_additional_v1_2_columns(df: DataFrame) -> DataFrame:
    """
    Transformations specific to the v1 and v2 survey responses.
    """
    df = update_column_values_from_map(
        df=df,
        column="is_self_isolating_detailed",
        map={
            "Yes for other reasons (e.g. going into hospital or quarantining)": "Yes, for other reasons (e.g. going into hospital, quarantining)",  # noqa: E501
            "Yes for other reasons related to reducing your risk of getting COVID-19 (e.g. going into hospital or shielding)": "Yes, for other reasons (e.g. going into hospital, quarantining)",  # noqa: E501
            "Yes for other reasons related to you having had an increased risk of getting COVID-19 (e.g. having been in contact with a known case or quarantining after travel abroad)": "Yes, for other reasons (e.g. going into hospital, quarantining)",  # noqa: E501
            "Yes because you live with someone who has/has had symptoms but you haven’t had them yourself": "Yes, someone you live with had symptoms",  # noqa: E501
            "Yes because you live with someone who has/has had symptoms or a positive test but you haven’t had symptoms yourself": "Yes, someone you live with had symptoms",  # noqa: E501
            "Yes because you live with someone who has/has had symptoms but you haven't had them yourself": "Yes, someone you live with had symptoms",  # noqa: E501
            "Yes because you have/have had symptoms of COVID-19": "Yes, you have/have had symptoms",
            "Yes because you have/have had symptoms of COVID-19 or a positive test": "Yes, you have/have had symptoms",
        },
    )
    df = assign_isin_list(
        df=df,
        column_name_to_assign="is_self_isolating",
        reference_column="is_self_isolating_detailed",
        values_list=[
            "Yes, for other reasons (e.g. going into hospital, quarantining)",
            "Yes, you have/have had symptoms",
            "Yes, someone you live with had symptoms",
        ],
        true_false_values=["Yes", "No"],
    )
    df = clean_within_range(df, "hours_a_day_with_someone_else_at_home", [0, 24])
    df = df.withColumn("been_outside_uk_last_country", F.upper(F.col("been_outside_uk_last_country")))

    df = assign_work_social_column(
        df,
        "work_social_care",
        "work_sectors",
        "work_nursing_or_residential_care_home",
        "work_direct_contact_patients_clients",
    )
    df = assign_work_health_care(
        df,
        "work_health_care_v0",
        direct_contact_column="work_direct_contact_patients_clients",
        health_care_column="work_health_care_v1_v2",
    )

    return df


def derive_age_columns(df: DataFrame, column_name_to_assign: str) -> DataFrame:
    """
    Transformations involving participant age.
    """
    df = assign_age_at_date(df, column_name_to_assign, base_date="visit_datetime", date_of_birth="date_of_birth")
    df = assign_named_buckets(
        df,
        reference_column=column_name_to_assign,
        column_name_to_assign="age_group_5_intervals",
        map={2: "2-11", 12: "12-19", 20: "20-49", 50: "50-69", 70: "70+"},
    )
    df = assign_named_buckets(
        df,
        reference_column=column_name_to_assign,
        column_name_to_assign="age_group_over_16",
        map={16: "16-49", 50: "50-70", 70: "70+"},
    )
    df = assign_named_buckets(
        df,
        reference_column=column_name_to_assign,
        column_name_to_assign="age_group_7_intervals",
        map={2: "2-11", 12: "12-16", 17: "17-25", 25: "25-34", 35: "35-49", 50: "50-69", 70: "70+"},
    )
    df = assign_named_buckets(
        df,
        reference_column=column_name_to_assign,
        column_name_to_assign="age_group_5_year_intervals",
        map={
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

    return df


def derive_work_status_columns(df: DataFrame) -> DataFrame:

    work_status_dict = {
        "work_status_v0": {
            "5y and older in full-time education": "Student",
            "Attending college or other further education provider (including apprenticeships) (including if temporarily absent)": "Student",  # noqa: E501
            "Employed and currently not working (e.g. on leave due to the COVID-19 pandemic (furloughed) or sick leave for 4 weeks or longer or maternity/paternity leave)": "Furloughed (temporarily not working)",  # noqa: E501
            "Self-employed and currently not working (e.g. on leave due to the COVID-19 pandemic (furloughed) or sick leave for 4 weeks or longer or maternity/paternity leave)": "Furloughed (temporarily not working)",  # noqa: E501
            "Self-employed and currently working (include if on leave or sick leave for less than 4 weeks)": "Self-employed",  # noqa: E501
            "Employed and currently working (including if on leave or sick leave for less than 4 weeks)": "Employed",  # noqa: E501
            "4-5y and older at school/home-school (including if temporarily absent)": "Student",  # noqa: E501
            "Not in paid work and not looking for paid work (include doing voluntary work here)": "Not working (unemployed, retired, long-term sick etc.)",  # noqa: E501
            "Not working and not looking for work (including voluntary work)": "Not working (unemployed, retired, long-term sick etc.)",
            "Retired (include doing voluntary work here)": "Not working (unemployed, retired, long-term sick etc.)",  # noqa: E501
            "Looking for paid work and able to start": "Not working (unemployed, retired, long-term sick etc.)",  # noqa: E501
            "Child under 4-5y not attending nursery or pre-school or childminder": "Student",  # noqa: E501
            "Self-employed and currently not working (e.g. on leave due to the COVID-19 pandemic or sick leave for 4 weeks or longer or maternity/paternity leave)": "Furloughed (temporarily not working)",  # noqa: E501
            "Child under 5y attending nursery or pre-school or childminder": "Student",  # noqa: E501
            "Child under 4-5y attending nursery or pre-school or childminder": "Student",  # noqa: E501
            "Retired": "Not working (unemployed, retired, long-term sick etc.)",  # noqa: E501
            "Attending university (including if temporarily absent)": "Student",  # noqa: E501
            "Not working and not looking for work": "Not working (unemployed, retired, long-term sick etc.)",  # noqa: E501
            "Child under 5y not attending nursery or pre-school or childminder": "Student",  # noqa: E501
        },
        "work_status_v1": {
            "Child under 5y attending child care": "Child under 5y attending child care",  # noqa: E501
            "Child under 5y attending nursery or pre-school or childminder": "Child under 5y attending child care",  # noqa: E501
            "Child under 4-5y attending nursery or pre-school or childminder": "Child under 5y attending child care",  # noqa: E501
            "Child under 5y not attending nursery or pre-school or childminder": "Child under 5y not attending child care",  # noqa: E501
            "Child under 5y not attending child care": "Child under 5y not attending child care",  # noqa: E501
            "Child under 4-5y not attending nursery or pre-school or childminder": "Child under 5y not attending child care",  # noqa: E501
            "Employed and currently not working (e.g. on leave due to the COVID-19 pandemic (furloughed) or sick leave for 4 weeks or longer or maternity/paternity leave)": "Employed and currently not working",  # noqa: E501
            "Employed and currently working (including if on leave or sick leave for less than 4 weeks)": "Employed and currently working",  # noqa: E501
            "Not working and not looking for work (including voluntary work)": "Not working and not looking for work",  # noqa: E501
            "Not in paid work and not looking for paid work (include doing voluntary work here)": "Not working and not looking for work",
            "Not working and not looking for work": "Not working and not looking for work",  # noqa: E501
            "Self-employed and currently not working (e.g. on leave due to the COVID-19 pandemic (furloughed) or sick leave for 4 weeks or longer or maternity/paternity leave)": "Self-employed and currently not working",  # noqa: E501
            "Self-employed and currently not working (e.g. on leave due to the COVID-19 pandemic or sick leave for 4 weeks or longer or maternity/paternity leave)": "Self-employed and currently not working",  # noqa: E501
            "Self-employed and currently working (include if on leave or sick leave for less than 4 weeks)": "Self-employed and currently working",  # noqa: E501
            "Retired (include doing voluntary work here)": "Retired",  # noqa: E501
            "Looking for paid work and able to start": "Looking for paid work and able to start",  # noqa: E501
            "Attending college or other further education provider (including apprenticeships) (including if temporarily absent)": "5y and older in full-time education",  # noqa: E501
            "Attending university (including if temporarily absent)": "5y and older in full-time education",  # noqa: E501
            "4-5y and older at school/home-school (including if temporarily absent)": "5y and older in full-time education",  # noqa: E501
        },
        "work_status_v2": {
            "Retired (include doing voluntary work here)": "Retired",  # noqa: E501
            "Attending college or other further education provider (including apprenticeships) (including if temporarily absent)": "Attending college or FE (including if temporarily absent)",  # noqa: E501
            "Attending university (including if temporarily absent)": "Attending university (including if temporarily absent)",  # noqa: E501
            "Child under 5y attending child care": "Child under 4-5y attending child care",  # noqa: E501
            "Child under 5y attending nursery or pre-school or childminder": "Child under 4-5y attending child care",  # noqa: E501
            "Child under 4-5y attending nursery or pre-school or childminder": "Child under 4-5y attending child care",  # noqa: E501
            "Child under 5y not attending nursery or pre-school or childminder": "Child under 4-5y not attending child care",  # noqa: E501
            "Child under 5y not attending child care": "Child under 4-5y not attending child care",  # noqa: E501
            "Child under 4-5y not attending nursery or pre-school or childminder": "Child under 4-5y not attending child care",  # noqa: E501
            "4-5y and older at school/home-school (including if temporarily absent)": "4-5y and older at school/home-school",  # noqa: E501
            "Employed and currently not working (e.g. on leave due to the COVID-19 pandemic (furloughed) or sick leave for 4 weeks or longer or maternity/paternity leave)": "Employed and currently not working",  # noqa: E501
            "Employed and currently working (including if on leave or sick leave for less than 4 weeks)": "Employed and currently working",  # noqa: E501
            "Not in paid work and not looking for paid work (include doing voluntary work here)": "Not working and not looking for work",  # noqa: E501
            "Not working and not looking for work (including voluntary work)": "Not working and not looking for work",  # noqa: E501
            "Self-employed and currently not working (e.g. on leave due to the COVID-19 pandemic or sick leave for 4 weeks or longer or maternity/paternity leave)": "Self-employed and currently not working",  # noqa: E501
            "Self-employed and currently not working (e.g. on leave due to the COVID-19 pandemic (furloughed) or sick leave for 4 weeks or longer or maternity/paternity leave)": "Self-employed and currently not working",  # noqa: E501
            "Self-employed and currently working (include if on leave or sick leave for less than 4 weeks)": "Self-employed and currently working",  # noqa: E501
            "5y and older in full-time education": "4-5y and older at school/home-school",  # noqa: E501
        },
    }

    column_list = ["work_status_v0", "work_status_v1"]
    for column in column_list:
        df = df.withColumn(column, F.col("work_status_v2"))
        df = update_column_values_from_map(df=df, column=column, map=work_status_dict[column])

    df = update_column_values_from_map(df=df, column="work_status_v2", map=work_status_dict["work_status_v2"])

    ## Not needed in release 1. Confirm that these are v2-only when pulling them back in, as they should likely be union dependent.
    # df = assign_work_person_facing_now(df, "work_person_facing_now", "work_person_facing_now", "work_social_care")
    # df = assign_column_given_proportion(
    #     df=df,
    #     column_name_to_assign="ever_work_person_facing_or_social_care",
    #     groupby_column="participant_id",
    #     reference_columns=["work_social_care"],
    #     count_if=["Yes, care/residential home, resident-facing", "Yes, other social care, resident-facing"],
    #     true_false_values=["Yes", "No"],
    # )
    # df = assign_column_given_proportion(
    #     df=df,
    #     column_name_to_assign="ever_care_home_worker",
    #     groupby_column="participant_id",
    #     reference_columns=["work_social_care", "work_nursing_or_residential_care_home"],
    #     count_if=["Yes, care/residential home, resident-facing"],
    #     true_false_values=["Yes", "No"],
    # )
    # df = assign_column_given_proportion(
    #     df=df,
    #     column_name_to_assign="ever_had_long_term_health_condition",
    #     groupby_column="participant_id",
    #     reference_columns=["illness_lasting_over_12_months"],
    #     count_if=["Yes"],
    #     true_false_values=["Yes", "No"],
    # )
    # df = assign_ever_had_long_term_health_condition_or_disabled(
    #     df=df,
    #     column_name_to_assign="ever_had_long_term_health_condition_or_disabled",
    #     health_conditions_column="illness_lasting_over_12_months",
    #     condition_impact_column="illness_reduces_activity_or_ability",
    # )
    return df


def clean_survey_responses_version_2(df: DataFrame) -> DataFrame:
    df = map_column_values_to_null(
        df=df,
        value="Participant Would Not/Could Not Answer",
        column_list=[
            "ethnicity",
            "work_sectors",
            "work_health_care_v1_v2",
            "work_status_v2",
            "work_location",
            "work_direct_contact_patients_clients",
            "work_nursing_or_residential_care_home",
            "visit_type",
            "household_visit_status",
            "participant_survey_status",
            "is_self_isolating_detailed",
            "ability_to_socially_distance_at_work_or_school",
            "transport_to_work_or_school",
            "face_covering_outside_of_home",
            "face_covering_work",
            "face_covering_other_enclosed_places",
            "other_antibody_test_location",
            "withdrawal_reason",
            "cis_covid_vaccine_type",
            "cis_covid_vaccine_number_of_doses",
            "work_not_from_home_days_per_week",
            "times_shopping_last_7_days",
            "times_socialise_last_7_days",
        ],
    )
    times_value_map = {"None": 0, "1": 1, "2": 2, "3": 3, "4": 4, "5": 5, "6": 6, "7 times or more": 7}
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
        "times_outside_shopping_or_socialising_last_7_days": times_value_map,
        "times_shopping_last_7_days": times_value_map,
        "times_socialise_last_7_days": times_value_map,
        "work_sectors": {
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
        "work_health_care_v1_v2": {
            "Primary Care (e.g. GP or dentist)": "Yes, in primary care, e.g. GP, dentist",
            "Primary care (e.g. GP or dentist)": "Yes, in primary care, e.g. GP, dentist",
            "Secondary Care (e.g. hospital)": "Yes, in secondary care, e.g. hospital",
            "Secondary care (e.g. hospital.)": "Yes, in secondary care, e.g. hospital",
            "Secondary care (e.g. hospital)": "Yes, in secondary care, e.g. hospital",
            "Other Healthcare (e.g. mental health)": "Yes, in other healthcare settings, e.g. mental health",
            "Other healthcare (e.g. mental health)": "Yes, in other healthcare settings, e.g. mental health",
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
        "face_covering_work": {
            "My face is already covered for other reasons (e.g. religious or cultural reasons)": "My face is already covered",
            "Yes always": "Yes, always",
            "Yes sometimes": "Yes, sometimes",
        },
        "other_antibody_test_results": {
            "One or more negative tests but none positive": "Any tests negative, but none negative",
            "One or more negative tests but none were positive": "Any tests negative, but none negative",
            "All tests failed": "All Tests failed",
        },
        "other_antibody_test_location": {
            "Private Lab": "Private lab",
            "Home Test": "Home test",
            "In the NHS (e.g. GP or hospital)": "In the NHS (e.g. GP, hospital)",
        },
        "other_pcr_test_results": {
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
        "is_self_isolating_detailed": {
            "Yes for other reasons (e.g. going into hospital or quarantining)": "Yes, for other reasons (e.g. going into hospital, quarantining)",
            "Yes for other reasons related to reducing your risk of getting COVID-19 (e.g. going into hospital or shielding)": "Yes, for other reasons (e.g. going into hospital, quarantining)",
            "Yes for other reasons related to you having had an increased risk of getting COVID-19 (e.g. having been in contact with a known case or quarantining after travel abroad)": "Yes, for other reasons (e.g. going into hospital, quarantining)",
            "Yes because you live with someone who has/has had symptoms but you haven’t had them yourself": "Yes, someone you live with had symptoms",
            "Yes because you live with someone who has/has had symptoms or a positive test but you haven’t had symptoms yourself": "Yes, someone you live with had symptoms",
            "Yes because you live with someone who has/has had symptoms but you haven't had them yourself": "Yes, someone you live with had symptoms",
            "Yes because you have/have had symptoms of COVID-19": "Yes, you have/have had symptoms",
            "Yes because you have/have had symptoms of COVID-19 or a positive test": "Yes, you have/have had symptoms",
        },
        "ability_to_socially_distance_at_work_or_school": {
            "Difficult to maintain 2 meters - but I can usually be at least 1m from other people": "Difficult to maintain 2m, but can be 1m",
            "Difficult to maintain 2m - but you can usually be at least 1m from other people": "Difficult to maintain 2m, but can be 1m",
            "Easy to maintain 2 meters - it is not a problem to stay this far away from other people": "Easy to maintain 2m",
            "Easy to maintain 2m - it is not a problem to stay this far away from other people": "Easy to maintain 2m",
            "Relatively easy to maintain 2 meters - most of the time I can be 2m away from other people": "Relatively easy to maintain 2m",
            "Relatively easy to maintain 2m - most of the time you can be 2m away from other people": "Relatively easy to maintain 2m",
            "Very difficult to be more than 1 meter away as my work means I am in close contact with others on a regular basis": "Very difficult to be more than 1m away",
            "Very difficult to be more than 1m away as your work means you are in close contact with others on a regular basis": "Very difficult to be more than 1m away",
        },
        "transport_to_work_or_school": {
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
        "last_covid_contact_location": {
            "In your own household": "Living in your own home",
            "Outside your household": "Outside your home",
        },
        "last_suspected_covid_contact_location": {
            "In your own household": "Living in your own home",
            "Outside your household": "Outside your home",
        },
    }
    df = apply_value_map_multiple_columns(df, column_editing_map)
    df = df.withColumn("deferred", F.when(F.col("deferred").isNull(), "NA").otherwise(F.col("deferred")))

    df = df.withColumn("swab_sample_barcode", F.upper(F.col("swab_sample_barcode")))
    df = df.withColumn("blood_sample_barcode", F.upper(F.col("blood_sample_barcode")))
    return df


def transform_survey_responses_version_2_delta(df: DataFrame) -> DataFrame:
    """
    Transformations that are specific to version 2 survey responses.
    """
    df = assign_column_uniform_value(df, "survey_response_dataset_major_version", 2)

    df = update_to_value_if_any_not_null(
        df,
        "cis_covid_vaccine_received",
        "Yes",
        [
            "cis_covid_vaccine_date",
            "cis_covid_vaccine_number_of_doses",
            "cis_covid_vaccine_type",
            "cis_covid_vaccine_type_other",
        ],
    )
    df = fill_forward_from_last_change(
        df=df,
        fill_forward_columns=[
            "cis_covid_vaccine_date",
            "cis_covid_vaccine_number_of_doses",
            "cis_covid_vaccine_type",
            "cis_covid_vaccine_type_other",
            "cis_covid_vaccine_received",
        ],
        participant_id_column="participant_id",
        visit_date_column="visit_datetime",
        record_changed_column="cis_covid_vaccine_received",
        record_changed_value="Yes",
    )

    df = edit_to_sum_or_max_value(
        df=df,
        column_name_to_assign="times_outside_shopping_or_socialising_last_7_days",
        columns_to_sum=[
            "times_shopping_last_7_days",
            "times_socialise_last_7_days",
        ],
        max_value=7,
    )
    df = derive_household_been_columns(
        df=df,
        column_name_to_assign="household_been_care_home_last_28_days",
        individual_response_column="care_home_last_28_days",
        household_response_column="care_home_last_28_days_other_household_member",
    )
    df = derive_household_been_columns(
        df=df,
        column_name_to_assign="household_been_hospital_last_28_days",
        individual_response_column="hospital_last_28_days",
        household_response_column="hospital_last_28_days_other_household_member",
    )
    df = derive_work_status_columns(df)
    return df


def symptom_column_transformations(df):
    df = count_value_occurrences_in_column_subset_row_wise(
        df=df,
        column_name_to_assign="symptoms_last_7_days_symptom_count",
        selection_columns=[
            "symptoms_last_7_days_fever",
            "symptoms_last_7_days_muscle_ache_myalgia",
            "symptoms_last_7_days_fatigue_weakness",
            "symptoms_last_7_days_sore_throat",
            "symptoms_last_7_days_cough",
            "symptoms_last_7_days_shortness_of_breath",
            "symptoms_last_7_days_headache",
            "symptoms_last_7_days_nausea_vomiting",
            "symptoms_last_7_days_abdominal_pain",
            "symptoms_last_7_days_diarrhoea",
            "symptoms_last_7_days_loss_of_taste",
            "symptoms_last_7_days_loss_of_smell",
            "symptoms_last_7_days_more_trouble_sleeping",
            "symptoms_last_7_days_chest_pain",
            "symptoms_last_7_days_palpitations",
            "symptoms_last_7_days_vertigo_dizziness",
            "symptoms_last_7_days_worry_anxiety",
            "symptoms_last_7_days_low_mood_not_enjoying_anything",
            "symptoms_last_7_days_memory_loss_or_confusion",
            "symptoms_last_7_days_difficulty_concentrating",
            "symptoms_last_7_days_runny_nose_sneezing",
            "symptoms_last_7_days_noisy_breathing_wheezing",
            "symptoms_last_7_days_loss_of_appetite",
        ],
        count_if_value="Yes",
    )
    df = count_value_occurrences_in_column_subset_row_wise(
        df=df,
        column_name_to_assign="symptoms_since_last_visit_count",
        selection_columns=[
            "symptoms_since_last_visit_fever",
            "symptoms_since_last_visit_muscle_ache_myalgia",
            "symptoms_since_last_visit_fatigue_weakness",
            "symptoms_since_last_visit_sore_throat",
            "symptoms_since_last_visit_cough",
            "symptoms_since_last_visit_shortness_of_breath",
            "symptoms_since_last_visit_headache",
            "symptoms_since_last_visit_nausea_vomiting",
            "symptoms_since_last_visit_abdominal_pain",
            "symptoms_since_last_visit_diarrhoea",
            "symptoms_since_last_visit_loss_of_taste",
            "symptoms_since_last_visit_loss_of_smell",
            "symptoms_since_last_visit_more_trouble_sleeping",
            "symptoms_since_last_visit_chest_pain",
            "symptoms_since_last_visit_palpitations",
            "symptoms_since_last_visit_vertigo_dizziness",
            "symptoms_since_last_visit_worry_anxiety",
            "symptoms_since_last_visit_low_mood_or_not_enjoying_anything",
            "symptoms_since_last_visit_memory_loss_or_confusion",
            "symptoms_since_last_visit_difficulty_concentrating",
            "symptoms_since_last_visit_runny_nose_sneezing",
            "symptoms_since_last_visit_noisy_breathing_wheezing",
            "symptoms_since_last_visit_loss_of_appetite",
        ],
        count_if_value="Yes",
    )
    # TODO - not needed until later release
    # df = update_symptoms_last_7_days_any(
    #     df=df,
    #     column_name_to_update="symptoms_last_7_days_any",
    #     count_reference_column="symptoms_last_7_days_symptom_count",
    # )

    # df = assign_true_if_any(
    #     df=df,
    #     column_name_to_assign="any_symptoms_last_7_days_or_now",
    #     reference_columns=["symptoms_last_7_days_any", "think_have_covid_symptoms"],
    #     true_false_values=["Yes", "No"],
    # )

    # df = assign_any_symptoms_around_visit(
    #     df=df,
    #     column_name_to_assign="any_symptoms_around_visit",
    #     symptoms_bool_column="any_symptoms_last_7_days_or_now",
    #     id_column="participant_id",
    #     visit_date_column="visit_datetime",
    #     visit_id_column="visit_id",
    # )

    # df = assign_true_if_any(
    #     df=df,
    #     column_name_to_assign="symptoms_last_7_days_cghfevamn_symptom_group",
    #     reference_columns=[
    #         "symptoms_last_7_days_cough",
    #         "symptoms_last_7_days_fever",
    #         "symptoms_last_7_days_loss_of_smell",
    #         "symptoms_last_7_days_loss_of_taste",
    #     ],
    #     true_false_values=["Yes", "No"],
    # )
    # df = assign_true_if_any(
    #     df=df,
    #     column_name_to_assign="think_have_covid_cghfevamn_symptom_group",
    #     reference_columns=[
    #         "symptoms_since_last_visit_cough",
    #         "symptoms_since_last_visit_fever",
    #         "symptoms_since_last_visit_loss_of_smell",
    #         "symptoms_since_last_visit_loss_of_taste",
    #     ],
    #     true_false_values=["Yes", "No"],
    # )
    # df = assign_true_if_any(
    #     df=df,
    #     column_name_to_assign="think_have_covid_cghfevamn_symptom_group",
    #     reference_columns=[
    #         "symptoms_since_last_visit_cough",
    #         "symptoms_since_last_visit_fever",
    #         "symptoms_since_last_visit_loss_of_smell",
    #         "symptoms_since_last_visit_loss_of_taste",
    #     ],
    #     true_false_values=["Yes", "No"],
    # )
    # df = assign_any_symptoms_around_visit(
    #     df=df,
    #     column_name_to_assign="symptoms_around_cghfevamn_symptom_group",
    #     id_column="participant_id",
    #     symptoms_bool_column="symptoms_last_7_days_cghfevamn_symptom_group",
    #     visit_date_column="visit_datetime",
    #     visit_id_column="visit_id",
    # )
    return df


def union_dependent_cleaning(df):
    col_val_map = {
        "ethnicity": {
            "African": "Black,Caribbean,African-African",
            "Caribbean": "Black,Caribbean,Afro-Caribbean",
            "Any other Black or African or Caribbean background": "Any other Black background",
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
            "Roma": "White-Gypsy or Irish Traveller",
            "White-Roma": "White-Gypsy or Irish Traveller",
            "Gypsy or Irish Traveller": "White-Gypsy or Irish Traveller",
            "Arab": "Other ethnic group-Arab",
        },
        "withdrawal_reason": {
            "Bad experience with tester / survey": "Bad experience with interviewer/survey",
            "Swab / blood process too distressing": "Swab/blood process too distressing",
            "Swab / blood process to distressing": "Swab/blood process too distressing",
            "Do NOT Reinstate": "Do not reinstate",
        },
    }
    df = apply_value_map_multiple_columns(df, col_val_map)
    df = convert_null_if_not_in_list(df, "sex", options_list=["Male", "Female"])
    # TODO: Add in once dependencies are derived
    # df = impute_latest_date_flag(
    #     df=df,
    #     participant_id_column="participant_id",
    #     visit_date_column="visit_date",
    #     visit_id_column="visit_id",
    #     contact_any_covid_column="contact_known_or_suspected_covid",
    #     contact_any_covid_date_column="contact_known_or_suspected_covid_latest_date",
    # )

    # TODO: Add in once dependencies are derived
    # df = assign_date_difference(
    #     df,
    #     "contact_known_or_suspected_covid_days_since",
    #     "contact_known_or_suspected_covid_latest_date",
    #     "visit_datetime",
    # )

    # TODO: add the following function once contact_known_or_suspected_covid_latest_date() is created
    # df = contact_known_or_suspected_covid_type(
    #     df=df,
    #     contact_known_covid_type_column='contact_known_covid_type',
    #     contact_any_covid_type_column='contact_any_covid_type',
    #     contact_any_covid_date_column='contact_any_covid_date',
    #     contact_known_covid_date_column='contact_known_covid_date',
    #     contact_suspect_covid_date_column='contact_suspect_covid_date',
    # )

    df = update_face_covering_outside_of_home(
        df=df,
        column_name_to_update="face_covering_outside_of_home",
        covered_enclosed_column="face_covering_other_enclosed_places",
        covered_work_column="face_covering_work",
    )

    return df


def union_dependent_derivations(df):
    """
    Transformations that must be carried out after the union of the different survey response schemas.
    """
    df = assign_fake_id(df, "ordered_household_id", "ons_household_id")
    df = symptom_column_transformations(df)
    df = create_formatted_datetime_string_columns(df)
    df = derive_age_columns(df, "age_at_visit")
    ethnicity_map = {
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
        df=df, column_name_to_assign="ethnicity_group", reference_column="ethnicity", map=ethnicity_map
    )
    df = assign_ethnicity_white(
        df, column_name_to_assign="ethnicity_white", ethnicity_group_column_name="ethnicity_group"
    )
    # df = assign_work_patient_facing_now(
    #     df, "work_patient_facing_now", age_column="age_at_visit", work_healthcare_column="work_health_care_v0"
    # )
    # df = update_work_facing_now_column(
    #     df,
    #     "work_patient_facing_now",
    #     "work_status_v0",
    #     ["Furloughed (temporarily not working)", "Not working (unemployed, retired, long-term sick etc.)", "Student"],
    # )
    # df = assign_first_visit(
    #     df=df,
    #     column_name_to_assign="household_first_visit_datetime",
    #     id_column="participant_id",
    #     visit_date_column="visit_datetime",
    # )
    # df = assign_last_visit(
    #     df=df,
    #     column_name_to_assign="last_attended_visit_datetime",
    #     id_column="participant_id",
    #     visit_status_column="participant_visit_status",
    #     visit_date_column="visit_datetime",
    # )
    # df = assign_date_difference(
    #     df=df,
    #     column_name_to_assign="days_since_enrolment",
    #     start_reference_column="household_first_visit_datetime",
    #     end_reference_column="last_attended_visit_datetime",
    # )
    # df = assign_date_difference(
    #     df=df,
    #     column_name_to_assign="household_weeks_since_survey_enrolment",
    #     start_reference_column="survey start",
    #     end_reference_column="visit_datetime",
    #     format="weeks",
    # )
    # df = assign_named_buckets(
    #     df,
    #     reference_column="days_since_enrolment",
    #     column_name_to_assign="visit_number",
    #     map={
    #         0: 0,
    #         4: 1,
    #         11: 2,
    #         18: 3,
    #         25: 4,
    #         43: 5,
    #         71: 6,
    #         99: 7,
    #         127: 8,
    #         155: 9,
    #         183: 10,
    #         211: 11,
    #         239: 12,
    #         267: 13,
    #         295: 14,
    #         323: 15,
    #     },
    # )
    # df = assign_any_symptoms_around_visit(
    #     df=df,
    #     column_name_to_assign="symptoms_around_cghfevamn_symptom_group",
    #     symptoms_bool_column="symptoms_last_7_days_cghfevamn_symptom_group",
    #     id_column="participant_id",
    #     visit_date_column="visit_datetime",
    #     visit_id_column="visit_id",
    # )
    df = derive_people_in_household_count(df)
    df = update_column_values_from_map(
        df=df,
        column="smokes_nothing_now",
        map={"Yes": "No", "No": "Yes"},
        condition_column="smokes_or_vapes",
    )
    df = df.withColumn(
        "study_cohort", F.when(F.col("study_cohort").isNull(), "Original").otherwise(F.col("study_cohort"))
    )

    df = fill_backwards_work_status_v2(
        df=df,
        date="visit_datetime",
        id="participant_id",
        fill_backward_column="work_status_v2",
        condition_column="work_status_v1",
        date_range=["2020-09-01", "2021-08-31"],
        condition_column_values=["5y and older in full-time education"],
        fill_only_backward_column_values=[
            "4-5y and older at school/home-school",
            "Attending college or FE (including if temporarily absent)",
            "Attending university (including if temporarily absent)",
        ],
    )
    df = assign_work_status_group(df, "work_status_group", "work_status_v0")
    return df


def derive_people_in_household_count(df):
    """
    Correct counts of household member groups and sum to get total number of people in household. Takes maximum
    final count by household for each record.
    """
    df = assign_household_participant_count(
        df,
        column_name_to_assign="household_participant_count",
        household_id_column="ons_household_id",
        participant_id_column="participant_id",
    )
    df = update_person_count_from_ages(
        df,
        column_name_to_assign="household_participants_not_consented_count",
        column_pattern=r"person_[1-9]_not_consenting_age",
    )
    df = update_person_count_from_ages(
        df,
        column_name_to_assign="household_participants_not_present_count",
        column_pattern=r"person_[1-8]_not_present_age",
    )
    df = assign_household_under_2_count(
        df,
        column_name_to_assign="household_participants_under_2_count",
        column_pattern=r"infant_[1-8]_age",
        condition_column="household_members_under_2_years",
    )
    household_window = Window.partitionBy("ons_household_id")

    household_participants = [
        "household_participant_count",
        "household_participants_not_consented_count",
        "household_participants_not_present_count",
        "household_participants_under_2_count",
    ]
    for household_participant_type in household_participants:
        df = df.withColumn(
            household_participant_type,
            F.max(household_participant_type).over(household_window),
        )
    df = df.withColumn(
        "people_in_household_count",
        sum_within_row(household_participants),
    )
    df = df.withColumn(
        "people_in_household_count_group",
        F.when(F.col("people_in_household_count") >= 5, "5+").otherwise(
            F.col("people_in_household_count").cast("string")
        ),
    )
    return df


def create_formatted_datetime_string_columns(df):
    """
    Create columns with specific datetime formatting for use in output data.
    """
    date_format_dict = {
        "visit_date_string": "visit_datetime",
        "samples_taken_date_string": "samples_taken_datetime",
    }
    datetime_format_dict = {
        "visit_datetime_string": "visit_datetime",
        "samples_taken_datetime_string": "samples_taken_datetime",
    }
    date_format_string_list = set(
        [
            "date_of_birth",
            "improved_visit_date",
            "think_had_covid_date",
            "cis_covid_vaccine_date",
            "cis_covid_vaccine_date_1",
            "cis_covid_vaccine_date_2",
            "cis_covid_vaccine_date_3",
            "cis_covid_vaccine_date_4",
            "last_suspected_covid_contact_date",
            "last_covid_contact_date",
            "other_pcr_test_first_positive_date",
            "other_antibody_test_last_negative_date",
            "other_antibody_test_first_positive_date",
            "other_pcr_test_last_negative_date",
            "been_outside_uk_last_date",
            "symptoms_last_7_days_onset_date",
        ]
        # TODO: Add back in once digital data is being included or make backwards compatible
        # + cis_digital_datetime_map["yyyy-MM-dd"]
    )
    for column_name_to_assign in date_format_dict.keys():
        df = assign_column_to_date_string(
            df=df,
            column_name_to_assign=column_name_to_assign,
            reference_column=date_format_dict[column_name_to_assign],
            time_format="ddMMMyyyy",
            lower_case=True,
        )
    for column_name_to_assign in date_format_string_list:
        df = assign_column_to_date_string(
            df=df,
            column_name_to_assign=column_name_to_assign + "_string",
            reference_column=column_name_to_assign,
            time_format="ddMMMyyyy",
            lower_case=True,
        )
    for column_name_to_assign in datetime_format_dict.keys():
        df = assign_column_to_date_string(
            df=df,
            column_name_to_assign=column_name_to_assign,
            reference_column=datetime_format_dict[column_name_to_assign],
            time_format="ddMMMyyyy HH:mm:ss",
            lower_case=True,
        )
    # TODO: Add back in once digital data is being included or make backwards compatible
    # for column_name_to_assign in cis_digital_datetime_map["yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"]:
    #     df = assign_column_to_date_string(
    #         df=df,
    #         column_name_to_assign=column_name_to_assign + "_string",
    #         reference_column=column_name_to_assign,
    #         time_format="ddMMMyyyy HH:mm:ss",
    #         lower_case=True,
    #     )
    return df


def fill_forwards_transformations(df):

    df = fill_backwards_overriding_not_nulls(
        df=df,
        column_identity="participant_id",
        ordering_column="visit_datetime",
        dataset_column="survey_response_dataset_major_version",
        column_list=["sex", "date_of_birth", "ethnicity"],
    )
    df = fill_forward_only_to_nulls_in_dataset_based_on_column(
        df=df,
        id="participant_id",
        date="visit_datetime",
        changed="work_main_job_changed",
        dataset="survey_response_dataset_major_version",
        dataset_value=2,
        list_fill_forward=[
            "work_main_job_title",
            "work_main_job_role",
            "work_sectors",
            "work_sectors_other",
            "work_social_care",
            "work_health_care_v0",
            "work_health_care_v1_v2",
            "work_nursing_or_residential_care_home",
            "work_direct_contact_patients_clients",
        ],
    )
    df = fill_forward_only_to_nulls(
        df=df,
        id="participant_id",
        date="visit_datetime",
        list_fill_forward=[
            "sex",
            "date_of_birth",
            "ethnicity",
        ],
    )

    # TODO: uncomment for releases after R1
    # df = fill_backwards_overriding_not_nulls(
    #     df=df,
    #     column_identity="participant_id",
    #     ordering_column="visit_date",
    #     dataset_column="survey_response_dataset_major_version",
    #     column_list=fill_forwards_and_then_backwards_list,
    # )

    ## TODO: Not needed until a future release, will leave commented out in code until required
    #
    #    df = update_column_if_ref_in_list(
    #        df=df,
    #        column_name_to_update="work_location",
    #        old_value=None,
    #        new_value="Not applicable, not currently working",
    #        reference_column="work_status_v0",
    #        check_list=[
    #            "Furloughed (temporarily not working)",
    #            "Not working (unemployed, retired, long-term sick etc.)",
    #            "Student",
    #        ],
    #    )
    df = update_to_value_if_any_not_null(
        df=df,
        column_name_to_assign="been_outside_uk_since_last_visit",
        value_to_assign="Yes",
        column_list=["been_outside_uk_last_country", "been_outside_uk_last_date"],
    )
    df = fill_forward_from_last_change(
        df=df,
        fill_forward_columns=[
            "been_outside_uk_last_country",
            "been_outside_uk_last_date",
            "been_outside_uk_since_last_visit",
        ],
        participant_id_column="participant_id",
        visit_date_column="visit_datetime",
        record_changed_column="been_outside_uk_since_last_visit",
        record_changed_value="Yes",
    )
    df = fill_backwards_overriding_not_nulls(
        df=df,
        column_identity="participant_id",
        ordering_column="visit_datetime",
        dataset_column="survey_response_dataset_major_version",
        column_list=["sex", "date_of_birth", "ethnicity"],
    )
    return df
