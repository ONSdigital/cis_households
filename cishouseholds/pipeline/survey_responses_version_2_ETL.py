from pyspark.sql import DataFrame

from cishouseholds.derive import assign_age_at_date
from cishouseholds.derive import assign_any_symptoms_around_visit
from cishouseholds.derive import assign_column_from_mapped_list_key
from cishouseholds.derive import assign_column_given_proportion
from cishouseholds.derive import assign_column_regex_match
from cishouseholds.derive import assign_column_to_date_string
from cishouseholds.derive import assign_column_uniform_value
from cishouseholds.derive import assign_consent_code
from cishouseholds.derive import assign_date_difference
from cishouseholds.derive import assign_ethnicity_white
from cishouseholds.derive import assign_ever_had_long_term_health_condition_or_disabled
from cishouseholds.derive import assign_filename_column
from cishouseholds.derive import assign_first_visit
from cishouseholds.derive import assign_grouped_variable_from_days_since
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
from cishouseholds.derive import count_value_occurrences_in_column_subset_row_wise
from cishouseholds.edit import clean_barcode
from cishouseholds.edit import clean_postcode
from cishouseholds.edit import convert_null_if_not_in_list
from cishouseholds.edit import format_string_upper_and_clean
from cishouseholds.edit import update_column_values_from_map
from cishouseholds.impute import impute_latest_date_flag


def transform_survey_responses_generic(df: DataFrame) -> DataFrame:
    """
    Generic transformation steps to be applied to all survey response records.
    """
    df = assign_filename_column(df, "survey_response_source_file")
    raw_copy_list = [
        "think_had_covid_any_symptoms",
        "symptoms_last_7_days_any",
        "work_main_job_title",
        "work_main_job_role",
        "work_health_care_v0",
        "work_health_care_v1_v2",
        "work_status_v0",
        "work_status_v1",
        "work_status_v2",
        "work_social_care",
        "work_not_from_home_days_per_week",
        "work_location",
        "sex",
        "withdrawal_reason",
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
        df, "consent", reference_columns=["consent_16_visits", "consent_5_visits", "consent_1_visit"]
    )
    df = clean_barcode(df=df, barcode_column="swab_sample_barcode")
    df = clean_barcode(df=df, barcode_column="blood_sample_barcode")
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

    df = assign_column_to_date_string(df, "visit_date_string", reference_column="visit_datetime")
    df = assign_column_to_date_string(df, "samples_taken_date_string", reference_column="samples_taken_datetime")
    df = assign_column_to_date_string(df, "date_of_birth_string", reference_column="date_of_birth")
    # df = assign_date_difference(
    #     df, "contact_known_or_suspected_covid_days_since", "contact_any_covid_date", "visit_datetime"
    # )
    df = assign_date_difference(df, "days_since_think_had_covid", "think_had_covid_date", "visit_datetime")
    df = convert_null_if_not_in_list(df, "sex", options_list=["Male", "Female"])
    df = assign_taken_column(df, "swab_taken", reference_column="swab_sample_barcode")
    df = assign_taken_column(df, "blood_taken", reference_column="blood_sample_barcode")
    df = assign_true_if_any(
        df=df,
        column_name_to_assign="think_have_covid_cghfevamn_symptom_group",
        reference_columns=[
            "symptoms_since_last_visit_cough",
            "symptoms_since_last_visit_fever",
            "symptoms_since_last_visit_loss_of_smell",
            "symptoms_since_last_visit_loss_of_taste",
        ],
        true_false_values=["Yes", "No"],
    )
    df = assign_true_if_any(
        df=df,
        column_name_to_assign="symptoms_last_7_days_cghfevamn_symptom_group",
        reference_columns=[
            "symptoms_last_7_days_cough",
            "symptoms_last_7_days_fever",
            "symptoms_last_7_days_loss_of_smell",
            "symptoms_last_7_days_loss_of_taste",
        ],
        true_false_values=["Yes", "No"],
    )
    df = assign_true_if_any(
        df=df,
        column_name_to_assign="think_have_covid_cghfevamn_symptom_group",
        reference_columns=[
            "symptoms_since_last_visit_cough",
            "symptoms_since_last_visit_fever",
            "symptoms_since_last_visit_loss_of_smell",
            "symptoms_since_last_visit_loss_of_taste",
        ],
        true_false_values=["Yes", "No"],
    )
    # df = assign_true_if_any(
    #     df=df,
    #     column_name_to_assign="any_symptoms_last_7_days_or_now",
    #     reference_columns=["symptoms_last_7_days_any", "think_have_covid_symptoms_now"],
    #     true_false_values=["Yes", "No"],
    # )
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
        ],
        count_if_value="Yes",
    )
    # df = assign_any_symptoms_around_visit(
    #     df=df,
    #     column_name_to_assign="any_symptoms_around_visit",
    #     symptoms_bool_column="any_symptoms_last_7_days_or_now",
    #     id_column="participant_id",
    #     visit_date_column="visit_datetime",
    #     visit_id_column="visit_id",
    # )
    # df = assign_any_symptoms_around_visit(
    #     df=df,
    #     column_name_to_assign="symptoms_around_cghfevamn_symptom_group",
    #     id_column="participant_id",
    #     symptoms_bool_column="symptoms_last_7_days_cghfevamn_symptom_group",
    #     visit_date_column="visit_datetime",
    #     visit_id_column="visit_id",
    # )

    # df = placeholder_for_derivation_number_17(df, "country_barcode", ["swab_barcode_cleaned","blood_barcode_cleaned"],
    #  {0:"ONS", 1:"ONW", 2:"ONN", 3:"ONC"})
    df = derive_age_columns(df)
    df = assign_grouped_variable_from_days_since(
        df=df,
        binary_reference_column="think_had_covid",
        days_since_reference_column="days_since_think_had_covid",
        column_name_to_assign="days_since_think_had_covid_group",
    )
    return df


def derive_additional_v1_2_columns(df: DataFrame) -> DataFrame:
    """
    Transformations specific to the v1 and 2 packages only
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
        column_name_to_assign="self_isolating",
        reference_column="is_self_isolating_detailed",
        values_list=[
            "Yes, for other reasons (e.g. going into hospital, quarantining)",
            "Yes, for other reasons (e.g. going into hospital, quarantining)",
            "Yes, for other reasons (e.g. going into hospital, quarantining)",
        ],
        true_false_values=["Yes", "No"],
    )

    return df


def derive_age_columns(df: DataFrame) -> DataFrame:
    """
    Transformations involving participant age.
    """
    df = assign_age_at_date(df, "age_at_visit", base_date="visit_datetime", date_of_birth="date_of_birth")
    df = assign_named_buckets(
        df,
        reference_column="age_at_visit",
        column_name_to_assign="age_group_5_intervals",
        map={2: "2-11", 12: "12-19", 20: "20-49", 50: "50-69", 70: "70+"},
    )
    df = assign_named_buckets(
        df,
        reference_column="age_at_visit",
        column_name_to_assign="age_group_over_16",
        map={16: "16-49", 50: "50-70", 70: "70+"},
    )
    df = assign_named_buckets(
        df,
        reference_column="age_at_visit",
        column_name_to_assign="age_group_7_intervals",
        map={2: "2-11", 12: "12-16", 17: "17-25", 25: "25-34", 35: "35-49", 50: "50-69", 70: "70+"},
    )
    df = assign_named_buckets(
        df,
        reference_column="age_at_visit",
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
    df = assign_school_year_september_start(
        df, dob_column="date_of_birth", visit_date="visit_datetime", column_name_to_assign="school_year_september"
    )
    # TODO: Enable once country data is linked on after merge
    # df = split_school_year_by_country(
    #   df, school_year_column = "school_year_september", country_column = "country_name"
    # )
    # df = assign_age_group_school_year(
    #   df, column_name_to_assign="age_group_school_year", country_column="country_name",
    #   age_column="age_at_visit", school_year_column="school_year_september"
    # )
    return df


def derive_work_status_columns(df: DataFrame) -> DataFrame:
    df = assign_work_social_column(
        df, "work_social_care", "work_sectors", "work_nursing_or_residential_care_home", "work_direct_contact_persons"
    )
    df = assign_work_person_facing_now(df, "work_person_facing_now", "work_person_facing_now", "work_social_care")
    # df = placeholder_for_derivation_number_23(df, "work_status", ["work_status_v1", "work_status_v2"])
    # df = placeholder_for_derivation_number_20(df, "work_healthcare",
    # ["work_healthcare_v1", "work_direct_contact"])
    # df = placeholder_for_derivation_number_21(df, "work_socialcare",
    # ["work_sector", "work_care_nursing_home" , "work_direct_contact"])
    # df = placeholder_for_derivation_number_10(df, "self_isolating", "self_isolating_v1")
    # df = placeholder_for_derivation_number_10(df, "contact_hospital",
    # ["contact_participant_hospital", "contact_other_in_hh_hospital"])
    # df = placeholder_for_derivation_number_10(df, "contact_carehome",
    # ["contact_participant_carehome", "contact_other_in_hh_carehome"])

    df = assign_column_given_proportion(
        df=df,
        column_name_to_assign="ever_work_person_facing_or_social_care",
        groupby_column="participant_id",
        reference_columns=["work_social_care"],
        count_if=["Yes, care/residential home, resident-facing", "Yes, other social care, resident-facing"],
        true_false_values=["Yes", "No"],
    )  # not sure of correct  PIPELINE categories
    df = assign_column_given_proportion(
        df=df,
        column_name_to_assign="ever_care_home_worker",
        groupby_column="participant_id",
        reference_columns=["work_social_care", "work_nursing_or_residential_care_home"],
        count_if=["Yes, care/residential home, resident-facing"],
        true_false_values=["Yes", "No"],
    )  # not sure of correct  PIPELINE categories
    df = assign_column_given_proportion(
        df=df,
        column_name_to_assign="ever_had_long_term_health_condition",
        groupby_column="participant_id",
        reference_columns=["illness_lasting_over_12_months"],
        count_if=["Yes"],
        true_false_values=["Yes", "No"],
    )  # not sure of correct  PIPELINE categories
    df = assign_ever_had_long_term_health_condition_or_disabled(
        df=df,
        column_name_to_assign="ever_had_long_term_health_condition_or_disabled",
        health_conditions_column="illness_lasting_over_12_months",
        condition_impact_column="illness_reduces_activity_or_ability",
    )
    return df


def transform_survey_responses_version_2_delta(df: DataFrame) -> DataFrame:
    """
    Transformations that are specific to version 2 survey responses.
    """
    df = assign_column_uniform_value(df, "survey_response_dataset_major_version", 1)
    df = format_string_upper_and_clean(df, "work_main_job_title")
    df = format_string_upper_and_clean(df, "work_main_job_role")
    return df


def union_dependent_transformations(df):
    """
    Transformations that must be carried out after the union of the different survey response schemas.
    """
    df = assign_work_health_care(
        df,
        "work_health_care_combined",
        direct_contact_column="work_direct_contact_patients_clients",
        reference_health_care_column="work_health_care_v0",
        other_health_care_column="work_health_care_v1_v2",
    )
    df = assign_work_patient_facing_now(
        df, "work_patient_facing_now", age_column="age_at_visit", work_healthcare_column="work_health_care_combined"
    )
    df = assign_first_visit(
        df=df,
        column_name_to_assign="household_first_visit_datetime",
        id_column="participant_id",
        visit_date_column="visit_datetime",
    )
    df = assign_last_visit(
        df=df,
        column_name_to_assign="last_attended_visit_datetime",
        id_column="participant_id",
        visit_status_column="participant_visit_status",
        visit_date_column="visit_datetime",
    )
    df = assign_date_difference(
        df=df,
        column_name_to_assign="days_since_enrolment",
        start_reference_column="household_first_visit_datetime",
        end_reference_column="last_attended_visit_datetime",
    )
    df = assign_date_difference(
        df=df,
        column_name_to_assign="household_weeks_since_survey_enrolment",
        start_reference_column="survey start",
        end_reference_column="visit_datetime",
        format="weeks",
    )
    # TODO: Add back in once work_status has been derived
    # df = update_work_facing_now_column(
    #     df,
    #     "work_patient_facing_now",
    #     "work_status",
    #     ["Furloughed (temporarily not working)", "Not working (unemployed, retired, long-term sick etc.)", "Student"],
    # )
    df = assign_named_buckets(
        df,
        reference_column="days_since_enrolment",
        column_name_to_assign="visit_number",
        map={
            0: 0,
            4: 1,
            11: 2,
            18: 3,
            25: 4,
            43: 5,
            71: 6,
            99: 7,
            127: 8,
            155: 9,
            183: 10,
            211: 11,
            239: 12,
            267: 13,
            295: 14,
            323: 15,
        },
    )
    df = assign_any_symptoms_around_visit(
        df=df,
        column_name_to_assign="symptoms_around_cghfevamn_symptom_group",
        symptoms_bool_column="sympt_now_cghfevamn",
        id_column="participant_id",
        visit_date_column="visit_date",
        visit_id_column="visit_id",
    )

    df = impute_latest_date_flag(
        df=df,
        participant_id_column="participant_id",
        visit_date_column="visit_date",
        visit_id_column="visit_id",
        contact_any_covid_column="contact_any_covid",
        contact_any_covid_date_column="contact_any_covid_date",
    )
    return df
