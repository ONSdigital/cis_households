# flake8: noqa
from functools import reduce
from operator import add
from operator import and_

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.dataframe import DataFrame

from cishouseholds.derive import assign_any_symptoms_around_visit
from cishouseholds.derive import assign_column_from_mapped_list_key
from cishouseholds.derive import assign_column_given_proportion
from cishouseholds.derive import assign_column_to_date_string
from cishouseholds.derive import assign_column_value_from_multiple_column_map
from cishouseholds.derive import assign_date_difference
from cishouseholds.derive import assign_ethnicity_white
from cishouseholds.derive import assign_ever_had_long_term_health_condition_or_disabled
from cishouseholds.derive import assign_fake_id
from cishouseholds.derive import assign_first_visit
from cishouseholds.derive import assign_household_participant_count
from cishouseholds.derive import assign_household_under_2_count
from cishouseholds.derive import assign_last_non_null_value_from_col_list
from cishouseholds.derive import assign_last_visit
from cishouseholds.derive import assign_named_buckets
from cishouseholds.derive import assign_true_if_any
from cishouseholds.derive import assign_visit_order
from cishouseholds.derive import assign_work_status_group
from cishouseholds.derive import contact_known_or_suspected_covid_type
from cishouseholds.derive import count_value_occurrences_in_column_subset_row_wise
from cishouseholds.derive import flag_records_for_childcare_v0_rules
from cishouseholds.derive import flag_records_for_childcare_v1_rules
from cishouseholds.derive import flag_records_for_childcare_v2_b_rules
from cishouseholds.derive import flag_records_for_college_v0_rules
from cishouseholds.derive import flag_records_for_college_v1_rules
from cishouseholds.derive import flag_records_for_college_v2_rules
from cishouseholds.derive import flag_records_for_furlough_rules_v0
from cishouseholds.derive import flag_records_for_furlough_rules_v1_a
from cishouseholds.derive import flag_records_for_furlough_rules_v1_b
from cishouseholds.derive import flag_records_for_furlough_rules_v2_a
from cishouseholds.derive import flag_records_for_furlough_rules_v2_b
from cishouseholds.derive import flag_records_for_not_working_rules_v0
from cishouseholds.derive import flag_records_for_not_working_rules_v1_a
from cishouseholds.derive import flag_records_for_not_working_rules_v1_b
from cishouseholds.derive import flag_records_for_not_working_rules_v2_a
from cishouseholds.derive import flag_records_for_not_working_rules_v2_b
from cishouseholds.derive import flag_records_for_retired_rules
from cishouseholds.derive import flag_records_for_school_v2_rules
from cishouseholds.derive import flag_records_for_self_employed_rules_v0
from cishouseholds.derive import flag_records_for_self_employed_rules_v1_a
from cishouseholds.derive import flag_records_for_self_employed_rules_v1_b
from cishouseholds.derive import flag_records_for_self_employed_rules_v2_a
from cishouseholds.derive import flag_records_for_self_employed_rules_v2_b
from cishouseholds.derive import flag_records_for_uni_v0_rules
from cishouseholds.derive import flag_records_for_uni_v1_rules
from cishouseholds.derive import flag_records_for_uni_v2_rules
from cishouseholds.derive import flag_records_for_work_from_home_rules
from cishouseholds.derive import flag_records_for_work_location_null
from cishouseholds.derive import flag_records_for_work_location_student
from cishouseholds.derive import regex_match_result
from cishouseholds.edit import apply_value_map_multiple_columns
from cishouseholds.edit import convert_null_if_not_in_list
from cishouseholds.edit import correct_date_ranges_union_dependent
from cishouseholds.edit import fuzzy_update
from cishouseholds.edit import remove_incorrect_dates
from cishouseholds.edit import replace_sample_barcode
from cishouseholds.edit import update_column_values_from_map
from cishouseholds.edit import update_face_covering_outside_of_home
from cishouseholds.edit import update_person_count_from_ages
from cishouseholds.edit import update_think_have_covid_symptom_any
from cishouseholds.edit import update_to_value_if_any_not_null
from cishouseholds.expressions import all_columns_values_in_list
from cishouseholds.expressions import any_column_equal_value
from cishouseholds.expressions import count_occurrence_in_row
from cishouseholds.expressions import sum_within_row
from cishouseholds.impute import fill_backwards_overriding_not_nulls
from cishouseholds.impute import fill_backwards_work_status_v2
from cishouseholds.impute import fill_forward_event
from cishouseholds.impute import fill_forward_from_last_change
from cishouseholds.impute import fill_forward_from_last_change_marked_subset
from cishouseholds.impute import fill_forward_only_to_nulls
from cishouseholds.impute import impute_and_flag
from cishouseholds.impute import impute_by_distribution
from cishouseholds.impute import impute_by_k_nearest_neighbours
from cishouseholds.impute import impute_by_mode
from cishouseholds.impute import impute_date_by_k_nearest_neighbours
from cishouseholds.impute import merge_previous_imputed_values
from cishouseholds.pipeline.mapping import date_cols_min_date_dict
from cishouseholds.pipeline.timestamp_map import cis_digital_datetime_map
from cishouseholds.regex.regex_patterns import at_school_pattern
from cishouseholds.regex.regex_patterns import at_university_pattern
from cishouseholds.regex.regex_patterns import childcare_pattern
from cishouseholds.regex.regex_patterns import furloughed_pattern
from cishouseholds.regex.regex_patterns import in_college_or_further_education_pattern
from cishouseholds.regex.regex_patterns import not_working_pattern
from cishouseholds.regex.regex_patterns import retired_regex_pattern
from cishouseholds.regex.regex_patterns import self_employed_regex
from cishouseholds.regex.regex_patterns import work_from_home_pattern


def fill_forwards_travel_column(df):
    df = update_to_value_if_any_not_null(
        df=df,
        column_name_to_assign="been_outside_uk",
        true_false_values=["Yes", "No"],
        column_list=["been_outside_uk_last_country", "been_outside_uk_last_return_date"],
    )
    df = fill_forward_from_last_change(
        df=df,
        fill_forward_columns=[
            "been_outside_uk_last_country",
            "been_outside_uk_last_return_date",
            "been_outside_uk",
        ],
        participant_id_column="participant_id",
        visit_datetime_column="visit_datetime",
        record_changed_column="been_outside_uk",
        record_changed_value="Yes",
    )
    return df


def fill_forwards_transformations(df):
    df = fill_forward_from_last_change_marked_subset(
        df=df,
        fill_forward_columns=[
            "work_main_job_title",
            "work_main_job_role",
            "work_sector",
            "work_sector_other",
            "work_social_care",
            "work_health_care_patient_facing",
            "work_health_care_area",
            "work_nursing_or_residential_care_home",
            "work_direct_contact_patients_or_clients",
        ],
        participant_id_column="participant_id",
        visit_datetime_column="visit_datetime",
        record_changed_column="work_main_job_changed",
        record_changed_value="Yes",
        dateset_version_column="survey_response_dataset_major_version",
        minimum_dateset_version=2,
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
    # df = update_column_if_ref_in_list(
    #     df=df,
    #     column_name_to_update="work_location",
    #     old_value=None,
    #     new_value="Not applicable, not currently working",
    #     reference_column="work_status_v0",
    #     check_list=[
    #         "Furloughed (temporarily not working)",
    #         "Not working (unemployed, retired, long-term sick etc.)",
    #         "Student",
    #     ],
    # )

    df = fill_forwards_travel_column(df)

    df = fill_backwards_overriding_not_nulls(
        df=df,
        column_identity="participant_id",
        ordering_column="visit_datetime",
        dataset_column="survey_response_dataset_major_version",
        column_list=["sex", "date_of_birth", "ethnicity"],
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
    return df


def union_dependent_cleaning(df):
    col_val_map = {
        "ethnicity": {
            "African": "Black,Caribbean,African-African",
            "Caribbean": "Black,Caribbean,Afro-Caribbean",
            "Any other Black or African or Caribbean background": "Any other Black background",
            "Any other Black| African| Carribbean": "Any other Black background",
            "Any other Mixed/Multiple background": "Any other Mixed background",
            "Bangladeshi": "Asian or Asian British-Bangladeshi",
            "Chinese": "Asian or Asian British-Chinese",
            "English, Welsh, Scottish, Northern Irish or British": "White-British",
            "English| Welsh| Scottish| Northern Irish or British": "White-British",
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
            "Any other white": "Any other white background",
        },
        "participant_withdrawal_reason": {
            "Bad experience with tester / survey": "Bad experience with interviewer/survey",
            "Swab / blood process too distressing": "Swab/blood process too distressing",
            "Swab / blood process to distressing": "Swab/blood process too distressing",
            "Do NOT Reinstate": "Do not reinstate",
        },
        "work_health_care_area": {
            "Secondary care for example in a hospital": "Secondary",
            "Another type of healthcare - for example mental health services?": "Other",
            "Primary care - for example in a GP or dentist": "Primary",
            "Yes, in primary care, e.g. GP, dentist": "Primary",
            "Secondary care - for example in a hospital": "Secondary",
            "Another type of healthcare - for example mental health services": "Other",  # noqa: E501
        },
    }
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
    df = correct_date_ranges_union_dependent(df, date_cols_to_correct, "participant_id", "visit_datetime", "visit_id")
    df = remove_incorrect_dates(df, date_cols_to_correct, "visit_datetime", "2019-08-01", date_cols_min_date_dict)

    df = apply_value_map_multiple_columns(df, col_val_map)
    df = convert_null_if_not_in_list(df, "sex", options_list=["Male", "Female"])

    df = fuzzy_update(
        df,
        id_column="participant_id",
        cols_to_check=[
            "other_covid_infection_test",
            "other_covid_infection_test_results",
            "think_had_covid_admitted_to_hospital",
            "think_had_covid_contacted_nhs",
        ],
        update_column="think_had_covid_onset_date",
        min_matches=3,
    )
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
        covered_work_column="face_covering_work_or_education",
    )
    return df


def symptom_column_transformations(df):
    df = count_value_occurrences_in_column_subset_row_wise(
        df=df,
        column_name_to_assign="think_have_covid_symptom_count",
        selection_columns=[
            "think_have_covid_symptom_fever",
            "think_have_covid_symptom_muscle_ache",
            "think_have_covid_symptom_fatigue",
            "think_have_covid_symptom_sore_throat",
            "think_have_covid_symptom_cough",
            "think_have_covid_symptom_shortness_of_breath",
            "think_have_covid_symptom_headache",
            "think_have_covid_symptom_nausea_or_vomiting",
            "think_have_covid_symptom_abdominal_pain",
            "think_have_covid_symptom_diarrhoea",
            "think_have_covid_symptom_loss_of_taste",
            "think_have_covid_symptom_loss_of_smell",
            "think_have_covid_symptom_more_trouble_sleeping",
            "think_have_covid_symptom_runny_nose_or_sneezing",
            "think_have_covid_symptom_noisy_breathing",
            "think_have_covid_symptom_loss_of_appetite",
        ],
        count_if_value="Yes",
    )
    df = count_value_occurrences_in_column_subset_row_wise(
        df=df,
        column_name_to_assign="think_had_covid_symptom_count",
        selection_columns=[
            "think_had_covid_symptom_fever",
            "think_had_covid_symptom_muscle_ache",
            "think_had_covid_symptom_fatigue",
            "think_had_covid_symptom_sore_throat",
            "think_had_covid_symptom_cough",
            "think_had_covid_symptom_shortness_of_breath",
            "think_had_covid_symptom_headache",
            "think_had_covid_symptom_nausea_or_vomiting",
            "think_had_covid_symptom_abdominal_pain",
            "think_had_covid_symptom_diarrhoea",
            "think_had_covid_symptom_loss_of_taste",
            "think_had_covid_symptom_loss_of_smell",
            "think_had_covid_symptom_more_trouble_sleeping",
            "think_had_covid_symptom_chest_pain",
            "think_had_covid_symptom_palpitations",
            "think_had_covid_symptom_vertigo_or_dizziness",
            "think_had_covid_symptom_anxiety",
            "think_had_covid_symptom_low_mood",
            "think_had_covid_symptom_memory_loss_or_confusion",
            "think_had_covid_symptom_difficulty_concentrating",
            "think_had_covid_symptom_runny_nose_or_sneezing",
            "think_had_covid_symptom_noisy_breathing",
            "think_had_covid_symptom_loss_of_appetite",
        ],
        count_if_value="Yes",
    )
    df = update_think_have_covid_symptom_any(
        df=df,
        column_name_to_update="think_have_covid_symptom_any",
    )

    df = assign_true_if_any(
        df=df,
        column_name_to_assign="any_think_have_covid_symptom_or_now",
        reference_columns=["think_have_covid_symptom_any", "think_have_covid"],
        true_false_values=["Yes", "No"],
    )

    df = assign_any_symptoms_around_visit(
        df=df,
        column_name_to_assign="any_symptoms_around_visit",
        symptoms_bool_column="any_think_have_covid_symptom_or_now",
        id_column="participant_id",
        visit_date_column="visit_datetime",
        visit_id_column="visit_id",
    )

    df = assign_true_if_any(
        df=df,
        column_name_to_assign="think_have_covid_cghfevamn_symptom_group",
        reference_columns=[
            "think_have_covid_symptom_cough",
            "think_have_covid_symptom_fever",
            "think_have_covid_symptom_loss_of_smell",
            "think_have_covid_symptom_loss_of_taste",
        ],
        true_false_values=["Yes", "No"],
    )
    df = assign_true_if_any(
        df=df,
        column_name_to_assign="think_had_covid_cghfevamn_symptom_group",
        reference_columns=[
            "think_had_covid_symptom_cough",
            "think_had_covid_symptom_fever",
            "think_had_covid_symptom_loss_of_smell",
            "think_had_covid_symptom_loss_of_taste",
        ],
        true_false_values=["Yes", "No"],
    )
    # df = assign_true_if_any(
    #     df=df,
    #     column_name_to_assign="think_have_covid_cghfevamn_symptom_group",
    #     reference_columns=[
    #         "think_had_covid_symptom_cough",
    #         "think_had_covid_symptom_fever",
    #         "think_had_covid_symptom_loss_of_smell",
    #         "think_had_covid_symptom_loss_of_taste",
    #     ],
    #     true_false_values=["Yes", "No"],
    # )
    df = assign_any_symptoms_around_visit(
        df=df,
        column_name_to_assign="symptoms_around_cghfevamn_symptom_group",
        id_column="participant_id",
        symptoms_bool_column="think_have_covid_cghfevamn_symptom_group",
        visit_date_column="visit_datetime",
        visit_id_column="visit_id",
    )
    return df


def create_ever_variable_columns(df: DataFrame) -> DataFrame:
    df = assign_column_given_proportion(
        df=df,
        column_name_to_assign="ever_work_person_facing_or_social_care",
        groupby_column="participant_id",
        reference_columns=["work_social_care"],
        count_if=["Yes, care/residential home, resident-facing", "Yes, other social care, resident-facing", "Yes"],
        true_false_values=["Yes", "No"],
    )
    df = assign_column_given_proportion(
        df=df,
        column_name_to_assign="ever_care_home_worker",
        groupby_column="participant_id",
        reference_columns=["work_social_care", "work_nursing_or_residential_care_home"],
        count_if=["Yes", "Yes, care/residential home, resident-facing"],
        true_false_values=["Yes", "No"],
    )
    df = assign_column_given_proportion(
        df=df,
        column_name_to_assign="ever_had_long_term_health_condition",
        groupby_column="participant_id",
        reference_columns=["illness_lasting_over_12_months"],
        count_if=["Yes"],
        true_false_values=["Yes", "No"],
    )
    df = assign_ever_had_long_term_health_condition_or_disabled(
        df=df,
        column_name_to_assign="ever_had_long_term_health_condition_or_disabled",
        health_conditions_column="illness_lasting_over_12_months",
        condition_impact_column="illness_reduces_activity_or_ability",
    )
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
        column_name_to_assign="household_participants_not_consenting_count",
        column_pattern=r"person_not_consenting_age_[1-9]",
    )
    df = update_person_count_from_ages(
        df,
        column_name_to_assign="household_members_over_2_years_and_not_present_count",
        column_pattern=r"person_not_present_age_[1-8]",
    )
    df = assign_household_under_2_count(
        df,
        column_name_to_assign="household_members_under_2_years_count",
        column_pattern=r"infant_age_months_[1-9]",
        condition_column="household_members_under_2_years",
    )
    household_window = Window.partitionBy("ons_household_id")

    household_participants = [
        "household_participant_count",
        "household_participants_not_consenting_count",
        "household_members_over_2_years_and_not_present_count",
        "household_members_under_2_years_count",
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


def clean_covid_test_swab(df: DataFrame):
    """
    Clean all variables related to the swab covid test.
    """
    df = df.withColumn(
        "other_covid_infection_test_results",
        F.when(
            (
                (F.col("other_covid_infection_test_results") == "Negative")
                & (F.col("think_had_covid_onset_date").isNull())
                & (F.col("think_had_covid_symptom_count") == 0)
            ),
            None,
        ).otherwise(F.col("other_covid_infection_test_results")),
    )

    # if the participant sais they have not had another covid test but there is a result for the test
    # and covid symptoms present or a date where symptoms occured exists or the user has been involved with a hospital
    # due to covid then set to 'Yes'
    df = df.withColumn(
        "other_covid_infection_test",
        F.when(
            (~F.col("other_covid_infection_test").eqNullSafe("Yes"))
            & (F.col("other_covid_infection_test_results").isNotNull())
            & (
                ((F.col("think_had_covid_symptom_count") > 0) | (F.col("think_had_covid_onset_date").isNotNull()))
                | (
                    (F.col("think_had_covid_admitted_to_hospital") == "Yes")
                    & (F.col("think_had_covid_contacted_nhs") == "Yes")
                )
            ),
            "Yes",
        ).otherwise(F.col("other_covid_infection_test")),
    )
    df.cache()

    # Reset no (0) to missing where ‘No’ overall and random ‘No’s given for other covid variables.
    flag = (
        (F.col("think_had_covid_symptom_count") == 0)
        & (~F.col("other_covid_infection_test_results").eqNullSafe("Positive"))
        & reduce(
            and_,
            (
                (~F.col(c).eqNullSafe("Yes"))
                for c in [
                    "think_had_covid",
                    "think_had_covid_contacted_nhs",
                    "think_had_covid_admitted_to_hospital",
                    "other_covid_infection_test",
                ]
            ),
        )
    )
    for col in ["think_had_covid_contacted_nhs", "think_had_covid_admitted_to_hospital"]:
        df = df.withColumn(col, F.when(flag, None).otherwise(F.col(col)))

    for col in ["other_covid_infection_test", "other_covid_infection_test_results"]:
        df = df.withColumn(
            col,
            F.when((flag) & (F.col("survey_response_dataset_major_version") == 0), None).otherwise(F.col(col)),
        )

    # Clean where date and/or symptoms are present, but ‘feeder question’ is no to thinking had covid.

    df = df.withColumn(
        "think_had_covid",
        F.when(
            (F.col("think_had_covid_onset_date").isNotNull()) | (F.col("think_had_covid_symptom_count") > 0), "Yes"
        ).otherwise(F.col("think_had_covid")),
    )

    df.unpersist()
    df.cache()

    # Clean where admitted is 1 but no to ‘feeder question’ for v0 dataset.

    for col in ["think_had_covid", "think_had_covid_admitted_to_hospital"]:
        df = df.withColumn(
            col,
            F.when(
                (F.col("think_had_covid_admitted_to_hospital") == "Yes")
                & (~F.col("think_had_covid_contacted_nhs").eqNullSafe("Yes"))
                & (~F.col("other_covid_infection_test").eqNullSafe("Yes"))
                & (F.col("think_had_covid_symptom_count") == 0)
                & (~F.col("other_covid_infection_test_results").eqNullSafe("Positive")),
                "No",
            ).otherwise(F.col(col)),
        )

    for col in ["think_had_covid_admitted_to_hospital", "think_had_covid_contacted_nhs"]:
        df = df.withColumn(
            col,
            F.when(
                (F.col("think_had_covid") == "No")
                & (F.col("think_had_covid_admitted_to_hospital") == "Yes")
                & (F.col("think_had_covid_contacted_nhs") == "Yes")
                & (~F.col("other_covid_infection_test").eqNullSafe("Yes"))
                & (F.col("other_covid_infection_test_results").isNull())
                & (F.col("think_had_covid_onset_date").isNull())
                & (F.col("think_had_covid_symptom_count") == 0)
                & (F.col("survey_response_dataset_major_version") == 0),
                "No",
            ).otherwise(F.col(col)),
        )

    for col in ["think_had_covid_admitted_to_hospital", "other_covid_infection_test", "think_had_covid"]:
        df = df.withColumn(
            col,
            F.when(
                F.col("think_had_covid").isNull()
                & (F.col("think_had_covid_admitted_to_hospital") == "Yes")
                & (~F.col("think_had_covid_contacted_nhs").eqNullSafe("Yes"))
                & (F.col("other_covid_infection_test") == "Yes")
                & (F.col("other_covid_infection_test_results").isNull())
                & (F.col("think_had_covid_onset_date").isNull())
                & (F.col("think_had_covid_symptom_count") == 0)
                & (F.col("survey_response_dataset_major_version") == 0),
                "No",
            ).otherwise(F.col(col)),
        )

    # Clean where admitted is 1 but no to ‘feeder question’.

    df = df.withColumn(
        "think_had_covid",
        F.when(
            (F.col("think_had_covid") != "Yes")
            & (F.col("think_had_covid_admitted_to_hospital") == "Yes")
            & (F.col("think_had_covid_symptom_count") == 0)
            & (
                (F.col("think_had_covid_contacted_nhs") != "Yes")
                | (F.col("other_covid_infection_test_results").isNotNull())
            )
            & (F.col("other_covid_infection_test") == "Yes"),
            "Yes",
        ).otherwise(F.col("think_had_covid")),
    )
    df.unpersist()
    return df


def clean_covid_event_detail_cols(df):
    think_had_covid_cols = [
        "survey_response_dataset_major_version",
        "think_had_covid_admitted_to_hospital",
        "think_had_covid",
        "think_had_covid_contacted_nhs",
        "think_have_covid_symptom_count",
        "think_had_covid_onset_date",
    ]
    hospital_covid_cols = ["think_had_covid_contacted_nhs", "think_had_covid_admitted_to_hospital"]
    other_covid_test_cols = ["other_covid_infection_test", "other_covid_infection_test_results"]
    covid_test_cols = [*hospital_covid_cols, *other_covid_test_cols]
    # 1
    df = assign_column_value_from_multiple_column_map(
        df,
        "think_had_covid_admitted_to_hospital",
        [
            [
                None,
                [1, "No", "No", "No", 0, None],
            ]
        ],
        think_had_covid_cols,
        override_original=False,
    )
    # 2
    df = assign_column_value_from_multiple_column_map(
        df,
        "think_had_covid_contacted_nhs",
        [
            [
                None,
                [1, None, "No", "No", 0, None],
            ]
        ],
        think_had_covid_cols,
        override_original=False,
    )
    # 3
    df.cache()
    for col in ["think_had_covid_contacted_nhs", "think_had_covid_admitted_to_hospital", "other_covid_infection_test"]:
        df = df.withColumn(
            col,
            F.when(
                (~F.col("think_had_covid").eqNullSafe("Yes"))
                & (F.col("other_covid_infection_test_results").isNull())
                & (F.col("survey_response_dataset_major_version") == 0)
                & (
                    reduce(
                        add,
                        [
                            F.when(F.col(c).isin(["Yes", "One or more positive test(s)"]), 1).otherwise(0)
                            for c in covid_test_cols
                        ],
                    )
                    == 1
                ),
                None,
            ).otherwise(F.col(col)),
        )
    # 4
    df = assign_column_value_from_multiple_column_map(
        df,
        "think_had_covid",
        [
            [
                "No",
                [0, "No", None, ["No", None], 0, None, "Yes", "Any tests negative, but none positive"],
            ],
        ],
        [*think_had_covid_cols, "other_covid_infection_test", "other_covid_infection_test_results"],
        override_original=False,
    )
    # 5
    df = assign_column_value_from_multiple_column_map(
        df,
        "think_had_covid",
        [["Yes", ["No", "Yes", "One or more positive test(s)", 0]]],
        [
            "think_had_covid",
            "other_covid_infection_test",
            "other_covid_infection_test_results",
            "survey_response_dataset_major_version",
        ],
        override_original=False,
    )
    # 6
    for col in covid_test_cols:
        df = df.withColumn(
            col,
            F.when(
                (all_columns_values_in_list(covid_test_cols, ["No", "Any tests negative, but none positive", None]))
                & (F.col("think_had_covid") == "Yes")
                & (F.col("think_had_covid_onset_date").isNull())
                & (F.col("survey_response_dataset_major_version") == 0),
                None,
            ).otherwise(F.col(col)),
        )
    # 7
    df = assign_column_value_from_multiple_column_map(
        df,
        "other_covid_infection_test",
        [[None, ["No", "No", None, None, None, 0, 0]]],
        [
            "other_covid_infection_test",
            "think_had_covid",
            "think_had_covid_contacted_nhs",
            "think_had_covid_admitted_to_hospital",
            "other_covid_infection_test_results",
            "think_have_covid_symptom_count",
            "survey_response_dataset_major_version",
        ],
        override_original=False,
    )
    # 8
    df.cache()
    for col in hospital_covid_cols:
        df = df.withColumn(
            col,
            F.when(
                (F.col(col) == "No")
                & (F.col("think_had_covid") == "No")
                & (F.col("think_had_covid_onset_date").isNull())
                & (F.col("think_have_covid_symptom_count") == 0)
                & (F.col("survey_response_dataset_major_version") == 1)
                & (
                    reduce(
                        add,
                        [
                            F.when(F.col(c).isin(["No", "Any tests negative, but none positive", None]), 1).otherwise(0)
                            for c in hospital_covid_cols
                        ],
                    )
                    == 1
                ),
                None,
            ).otherwise(F.col(col)),
        )
    # 9
    count_no = reduce(
        add,
        [
            *[F.when(F.col(c).eqNullSafe("No"), 1).otherwise(0) for c in hospital_covid_cols],
            F.when(
                F.col("other_covid_infection_test") == "No", F.col("survey_response_dataset_major_version")
            ).otherwise(0),
        ],
    )
    count_yes = count_occurrence_in_row([*hospital_covid_cols, "other_covid_infection_test"], "Yes")
    for col in [*hospital_covid_cols, "other_covid_infection_test"]:
        df = df.withColumn(
            col,
            F.when(
                (F.col(col).eqNullSafe("No"))
                & (F.col("think_had_covid").isNull())
                & (count_no <= 2)
                & (F.col("other_covid_infection_test_results").isNull())
                & (F.col("survey_response_dataset_major_version") == 0),
                None,
            ).otherwise(F.col(col)),
        )
    # 10
    df = assign_column_value_from_multiple_column_map(
        df,
        "other_covid_infection_test_results",
        [[None, ["Any tests negative, but none positive", None, None, None, None, "No", 0]]],
        [
            "other_covid_infection_test_results",
            "think_had_covid",
            "think_had_covid_onset_date",
            "think_had_covid_contacted_nhs",
            "think_had_covid_admitted_to_hospital",
            "other_covid_infection_test",
            "survey_response_dataset_major_version",
        ],
        override_original=False,
    )
    # 11
    df = df.withColumn(
        "think_had_covid",
        F.when(
            (F.col("think_had_covid").isNull())
            & (count_no >= 3)
            & (count_yes == 0)
            & (F.col("other_covid_infection_test_results").isNull())
            & (F.col("think_had_covid_onset_date").isNull())
            & (F.col("survey_response_dataset_major_version") == 0),
            "No",
        ).otherwise(F.col("think_had_covid")),
    )
    # 12
    for col in covid_test_cols:
        df = assign_column_value_from_multiple_column_map(
            df,
            col,
            [
                [
                    None,
                    [
                        ["Any tests negative, but none positive", None],
                        "No",
                        None,
                        [None, "No"],
                        [None, "No"],
                        [None, "No"],
                        0,
                        0,
                    ],
                ]
            ],
            [
                "other_covid_infection_test_results",
                "think_had_covid",
                "think_had_covid_onset_date",
                "think_had_covid_contacted_nhs",
                "think_had_covid_admitted_to_hospital",
                "other_covid_infection_test",
                "survey_response_dataset_major_version",
                "think_have_covid_symptom_count",
            ],
            override_original=False,
        )
    # 13
    for col in hospital_covid_cols:
        df = assign_column_value_from_multiple_column_map(
            df,
            col,
            [[None, [["Any tests negative, but none positive", None], "No", None, [None, "No"], "Yes", 0, 0]]],
            [
                "other_covid_infection_test_results",
                "think_had_covid",
                "think_had_covid_onset_date",
                "think_had_covid_admitted_to_hospital",
                "other_covid_infection_test",
                "survey_response_dataset_major_version",
                "think_have_covid_symptom_count",
            ],
            override_original=False,
        )
        df.unpersist()
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
    date_format_string_list = [
        "date_of_birth",
        "improved_visit_date",
        "think_had_covid_onset_date",
        "cis_covid_vaccine_date",
        "cis_covid_vaccine_date_1",
        "cis_covid_vaccine_date_2",
        "cis_covid_vaccine_date_3",
        "cis_covid_vaccine_date_4",
        "last_suspected_covid_contact_date",
        "last_covid_contact_date",
        "other_covid_infection_test_first_positive_date",
        "other_antibody_test_last_negative_date",
        "other_antibody_test_first_positive_date",
        "other_covid_infection_test_last_negative_date",
        "been_outside_uk_last_return_date",
        "think_have_covid_onset_date",
        "swab_return_date",
        "swab_return_future_date",
        "blood_return_date",
        "blood_return_future_date",
        "cis_covid_vaccine_date_5",
        "cis_covid_vaccine_date_6",
        "cis_covid_vaccine_date",
        "think_have_covid_symptom_onset_date",  # tempvar
        "other_covid_infection_test_positive_date",  # tempvar
        "other_covid_infection_test_negative_date",  # tempvar
        "other_antibody_test_positive_date",  # tempvar
        "other_antibody_test_negative_date",  # tempvar
    ]
    date_format_string_list = [
        col for col in date_format_string_list if col not in cis_digital_datetime_map["yyyy-MM-dd"]
    ] + cis_digital_datetime_map["yyyy-MM-dd"]

    for column_name_to_assign, timestamp_column in date_format_dict.items():
        if timestamp_column in df.columns:
            df = assign_column_to_date_string(
                df=df,
                column_name_to_assign=column_name_to_assign,
                reference_column=timestamp_column,
                time_format="ddMMMyyyy",
                lower_case=True,
            )
    for timestamp_column in date_format_string_list:
        if timestamp_column in df.columns:
            df = assign_column_to_date_string(
                df=df,
                column_name_to_assign=timestamp_column + "_string",
                reference_column=timestamp_column,
                time_format="ddMMMyyyy",
                lower_case=True,
            )
    for column_name_to_assign, timestamp_column in datetime_format_dict.items():
        if timestamp_column in df.columns:
            df = assign_column_to_date_string(
                df=df,
                column_name_to_assign=column_name_to_assign,
                reference_column=timestamp_column,
                time_format="ddMMMyyyy HH:mm:ss",
                lower_case=True,
            )
    for timestamp_column in cis_digital_datetime_map["yyyy-MM-dd'T'HH:mm:ss'Z'"]:
        if timestamp_column in df.columns:
            df = assign_column_to_date_string(
                df=df,
                column_name_to_assign=timestamp_column + "_string",
                reference_column=timestamp_column,
                time_format="ddMMMyyyy HH:mm:ss",
                lower_case=True,
            )
    return df


def union_dependent_derivations(df):
    """
    Transformations that must be carried out after the union of the different survey response schemas.
    """
    df = assign_fake_id(df, "ordered_household_id", "ons_household_id")
    df = assign_visit_order(
        df=df,
        column_name_to_assign="visit_order",
        id="participant_id",
        order_list=["visit_datetime", "visit_id"],
    )
    df = symptom_column_transformations(df)
    if "survey_completion_status" in df.columns:
        df = df.withColumn(
            "participant_visit_status", F.coalesce(F.col("participant_visit_status"), F.col("survey_completion_status"))
        )
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

    df = replace_sample_barcode(df=df)

    df = assign_column_from_mapped_list_key(
        df=df, column_name_to_assign="ethnicity_group", reference_column="ethnicity", map=ethnicity_map
    )
    df = assign_ethnicity_white(
        df, column_name_to_assign="ethnicity_white", ethnicity_group_column_name="ethnicity_group"
    )

    df = create_ever_variable_columns(df)

    df = assign_first_visit(
        df=df,
        column_name_to_assign="household_first_visit_datetime",
        id_column="ons_household_id",
        visit_date_column="visit_datetime",
    )
    df = assign_last_visit(
        df=df,
        column_name_to_assign="last_attended_visit_datetime",
        id_column="ons_household_id",
        visit_status_column="participant_visit_status",
        visit_date_column="visit_datetime",
    )
    df = assign_date_difference(
        df=df,
        column_name_to_assign="days_since_enrolment",
        start_reference_column="household_first_visit_datetime",
        end_reference_column="visit_datetime",
    )
    df = assign_date_difference(
        df=df,
        column_name_to_assign="household_weeks_since_survey_enrolment",
        start_reference_column="survey start",
        end_reference_column="visit_datetime",
        format="weeks",
    )
    df = assign_named_buckets(
        df,
        reference_column="days_since_enrolment",
        column_name_to_assign="visit_number",
        map={**{0: 1, 14: 2, 21: 3, 28: 4}, **{i * 28: (i + 3) for i in range(2, 200)}},
    )
    df = assign_any_symptoms_around_visit(
        df=df,
        column_name_to_assign="symptoms_around_cghfevamn_symptom_group",
        symptoms_bool_column="think_have_covid_cghfevamn_symptom_group",
        id_column="participant_id",
        visit_date_column="visit_datetime",
        visit_id_column="visit_id",
    )
    df = derive_people_in_household_count(df)
    df = update_column_values_from_map(
        df=df,
        column="smokes_nothing_now",
        map={"Yes": "No", "No": "Yes"},
        condition_column="currently_smokes_or_vapes",
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

    window = Window.partitionBy("participant_id")
    patient_facing_percentage = F.sum(
        F.when(F.col("work_direct_contact_patients_or_clients") == "Yes", 1).otherwise(0)
    ).over(window) / F.sum(F.lit(1)).over(window)

    df = df.withColumn(
        "patient_facing_over_20_percent", F.when(patient_facing_percentage >= 0.2, "Yes").otherwise("No")
    )

    df = create_formatted_datetime_string_columns(df)

    df = clean_covid_test_swab(df)  # a25 stata logic
    df = clean_covid_event_detail_cols(df)  # a26 stata logic
    return df


def derive_contact_any_covid_covid_variables(df: DataFrame):
    """
    Derive variables related to combination of know and suspected covid data columns.
    """
    df = df.withColumn(
        "contact_known_or_suspected_covid",
        F.when(
            any_column_equal_value(
                ["contact_suspected_positive_covid_last_28_days", "contact_known_positive_covid_last_28_days"], "Yes"
            ),
            "Yes",
        ).otherwise("No"),
    )

    df = assign_last_non_null_value_from_col_list(
        df=df,
        column_name_to_assign="contact_known_or_suspected_covid_latest_date",
        column_list=["last_covid_contact_date", "last_suspected_covid_contact_date"],
    )

    df = contact_known_or_suspected_covid_type(
        df=df,
        contact_known_covid_type_column="last_covid_contact_type",
        contact_suspect_covid_type_column="last_suspected_covid_contact_type",
        contact_any_covid_type_column="contact_known_or_suspected_covid",
        contact_any_covid_date_column="contact_known_or_suspected_covid_latest_date",
        contact_known_covid_date_column="last_covid_contact_date",
        contact_suspect_covid_date_column="last_suspected_covid_contact_date",
    )

    df = assign_date_difference(
        df,
        "contact_known_or_suspected_covid_days_since",
        "contact_known_or_suspected_covid_latest_date",
        "visit_datetime",
    )

    return df


def fill_forward_events_for_key_columns(df):
    """
    Function that contains all fill_forward_event calls required to implement STATA-based last observation carried forward logic.
    """
    df = fill_forward_event(
        df=df,
        event_indicator_column="think_had_covid",
        event_date_column="think_had_covid_onset_date",
        event_date_tolerance=7,
        detail_columns=[
            "other_covid_infection_test",
            "other_covid_infection_test_results",
            "think_had_covid_any_symptoms",
            "think_had_covid_admitted_to_hospital",
            "think_had_covid_contacted_nhs",
            "think_had_covid_symptom_fever",
            "think_had_covid_symptom_muscle_ache",
            "think_had_covid_symptom_fatigue",
            "think_had_covid_symptom_sore_throat",
            "think_had_covid_symptom_cough",
            "think_had_covid_symptom_shortness_of_breath",
            "think_had_covid_symptom_headache",
            "think_had_covid_symptom_nausea_or_vomiting",
            "think_had_covid_symptom_abdominal_pain",
            "think_had_covid_symptom_loss_of_appetite",
            "think_had_covid_symptom_noisy_breathing",
            "think_had_covid_symptom_runny_nose_or_sneezing",
            "think_had_covid_symptom_more_trouble_sleeping",
            "think_had_covid_symptom_diarrhoea",
            "think_had_covid_symptom_loss_of_taste",
            "think_had_covid_symptom_loss_of_smell",
            "think_had_covid_symptom_memory_loss_or_confusion",
            "think_had_covid_symptom_chest_pain",
            "think_had_covid_symptom_vertigo_or_dizziness",
            "think_had_covid_symptom_difficulty_concentrating",
            "think_had_covid_symptom_anxiety",
            "think_had_covid_symptom_palpitations",
            "think_had_covid_symptom_low_mood",
        ],
        participant_id_column="participant_id",
        visit_datetime_column="visit_datetime",
        visit_id_column="visit_id",
    )
    # Derive these after fill forwards and other changes to dates
    df = fill_forward_event(
        df=df,
        event_indicator_column="contact_suspected_positive_covid_last_28_days",
        event_date_column="last_suspected_covid_contact_date",
        event_date_tolerance=7,
        detail_columns=["last_suspected_covid_contact_type"],
        participant_id_column="participant_id",
        visit_datetime_column="visit_datetime",
        visit_id_column="visit_id",
    )
    df = fill_forward_event(
        df=df,
        event_indicator_column="contact_known_positive_covid_last_28_days",
        event_date_column="last_covid_contact_date",
        event_date_tolerance=7,
        detail_columns=["last_covid_contact_type"],
        participant_id_column="participant_id",
        visit_datetime_column="visit_datetime",
        visit_id_column="visit_id",
    )
    df = derive_contact_any_covid_covid_variables(df)
    return df


def impute_key_columns(df: DataFrame, imputed_value_lookup_df: DataFrame, log_directory: str):
    """
    Impute missing values for key variables that are required for weight calibration.
    Most imputations require geographic data being joined onto the response records.

    Returns a single record per participant, with response values (when available) and missing values imputed.
    """
    unique_id_column = "participant_id"

    # Get latest record for each participant, assumes that they have been filled forwards
    participant_window = Window.partitionBy(unique_id_column).orderBy(F.col("visit_datetime").desc())
    deduplicated_df = (
        df.withColumn("ROW_NUMBER", F.row_number().over(participant_window))
        .filter(F.col("ROW_NUMBER") == 1)
        .drop("ROW_NUMBER")
    )

    if imputed_value_lookup_df is not None:
        deduplicated_df = merge_previous_imputed_values(deduplicated_df, imputed_value_lookup_df, unique_id_column)

    deduplicated_df = impute_and_flag(
        deduplicated_df,
        imputation_function=impute_by_mode,
        reference_column="ethnicity_white",
        group_by_column="ons_household_id",
    ).custom_checkpoint()

    deduplicated_df = impute_and_flag(
        deduplicated_df,
        impute_by_k_nearest_neighbours,
        reference_column="ethnicity_white",
        donor_group_columns=["cis_area_code_20"],
        donor_group_column_weights=[5000],
        log_file_path=log_directory,
    ).custom_checkpoint()

    deduplicated_df = impute_and_flag(
        deduplicated_df,
        imputation_function=impute_by_distribution,
        reference_column="sex",
        group_by_columns=["ethnicity_white", "region_code"],
        first_imputation_value="Female",
        second_imputation_value="Male",
    ).custom_checkpoint()

    deduplicated_df = impute_and_flag(
        deduplicated_df,
        impute_date_by_k_nearest_neighbours,
        reference_column="date_of_birth",
        donor_group_columns=["region_code", "people_in_household_count_group", "work_status_group"],
        log_file_path=log_directory,
    )

    return deduplicated_df.select(
        unique_id_column,
        *["ethnicity_white", "sex", "date_of_birth"],
        *[col for col in deduplicated_df.columns if col.endswith("_imputation_method")],
        *[col for col in deduplicated_df.columns if col.endswith("_is_imputed")],
    )


def reclassify_work_variables(
    df: DataFrame, spark_session: SparkSession, drop_original_variables: bool = True
) -> DataFrame:
    """
    Reclassify work-related variables based on rules & regex patterns

    Parameters
    ----------
    df
        The dataframe containing the work-status related variables we want to edit
    spark_session
        A active spark session - this is used to break lineage since the code generated
        in this function is very verbose, you may encounter memory error if we don't break
        lineage.
    drop_original_variables
        Set this to False if you want to retain the original variables so you can compare
        before & after edits.
    """
    # Work from Home
    working_from_home_regex_hit = regex_match_result(
        columns_to_check_in=["work_main_job_title", "work_main_job_role"],
        positive_regex_pattern=work_from_home_pattern.positive_regex_pattern,
        negative_regex_pattern=work_from_home_pattern.negative_regex_pattern,
    )
    # Rule_id: 1000
    update_work_location = flag_records_for_work_from_home_rules() & working_from_home_regex_hit

    # Furlough
    furlough_regex_hit = regex_match_result(
        columns_to_check_in=["work_main_job_title", "work_main_job_role"],
        positive_regex_pattern=furloughed_pattern.positive_regex_pattern,
        negative_regex_pattern=furloughed_pattern.negative_regex_pattern,
    )

    # Rule_id: 2000
    update_work_status_furlough_v0 = furlough_regex_hit & flag_records_for_furlough_rules_v0()
    # Rule_id: 2001
    update_work_status_furlough_v1_a = furlough_regex_hit & flag_records_for_furlough_rules_v1_a()
    # Rule_id: 2002
    update_work_status_furlough_v1_b = furlough_regex_hit & flag_records_for_furlough_rules_v1_b()
    # Rule_id: 2003
    update_work_status_furlough_v2_a = furlough_regex_hit & flag_records_for_furlough_rules_v2_a()
    # Rule_id: 2004
    update_work_status_furlough_v2_b = furlough_regex_hit & flag_records_for_furlough_rules_v2_b()

    # Self-Employed
    self_employed_regex_hit = regex_match_result(
        columns_to_check_in=["work_main_job_title", "work_main_job_role"],
        positive_regex_pattern=self_employed_regex.positive_regex_pattern,
        negative_regex_pattern=self_employed_regex.negative_regex_pattern,
    )

    # Rule_id: 3000
    update_work_status_self_employed_v0 = self_employed_regex_hit & flag_records_for_self_employed_rules_v0()
    # Rule_id: 3001
    update_work_status_self_employed_v1_a = self_employed_regex_hit & flag_records_for_self_employed_rules_v1_a()
    # Rule_id: 3002
    update_work_status_self_employed_v1_b = self_employed_regex_hit & flag_records_for_self_employed_rules_v1_b()
    # Rule_id: 3003
    update_work_status_self_employed_v2_a = self_employed_regex_hit & flag_records_for_self_employed_rules_v2_a()
    # Rule_id: 3004
    update_work_status_self_employed_v2_b = self_employed_regex_hit & flag_records_for_self_employed_rules_v2_b()

    # Retired
    retired_regex_hit = regex_match_result(
        columns_to_check_in=["work_main_job_title", "work_main_job_role"],
        positive_regex_pattern=retired_regex_pattern.positive_regex_pattern,
        negative_regex_pattern=retired_regex_pattern.negative_regex_pattern,
    )

    # Rule_id: 4000, 4001, 4002
    update_work_status_retired = retired_regex_hit | flag_records_for_retired_rules()

    # Not-working
    not_working_regex_hit = (
        regex_match_result(
            columns_to_check_in=["work_main_job_title", "work_main_job_role"],
            positive_regex_pattern=not_working_pattern.positive_regex_pattern,
            negative_regex_pattern=not_working_pattern.negative_regex_pattern,
        )
        & ~working_from_home_regex_hit  # type: ignore
    )

    # Rule_id: 5000
    update_work_status_not_working_v0 = not_working_regex_hit & flag_records_for_not_working_rules_v0()
    # Rule_id: 5001
    update_work_status_not_working_v1_a = not_working_regex_hit & flag_records_for_not_working_rules_v1_a()
    # Rule_id: 5002
    update_work_status_not_working_v1_b = not_working_regex_hit & flag_records_for_not_working_rules_v1_b()
    # Rule_id: 5003
    update_work_status_not_working_v2_a = not_working_regex_hit & flag_records_for_not_working_rules_v2_a()
    # Rule_id: 5004
    update_work_status_not_working_v2_b = not_working_regex_hit & flag_records_for_not_working_rules_v2_b()

    # School/Student
    school_regex_hit = regex_match_result(
        columns_to_check_in=["work_main_job_title", "work_main_job_role"],
        positive_regex_pattern=at_school_pattern.positive_regex_pattern,
        negative_regex_pattern=at_school_pattern.negative_regex_pattern,
    )

    college_regex_hit = regex_match_result(
        columns_to_check_in=["work_main_job_title", "work_main_job_role"],
        positive_regex_pattern=in_college_or_further_education_pattern.positive_regex_pattern,
        negative_regex_pattern=in_college_or_further_education_pattern.negative_regex_pattern,
    )

    university_regex_hit = regex_match_result(
        columns_to_check_in=["work_main_job_title", "work_main_job_role"],
        positive_regex_pattern=at_university_pattern.positive_regex_pattern,
        negative_regex_pattern=at_university_pattern.negative_regex_pattern,
    )

    # Childcare
    childcare_regex_hit = regex_match_result(
        columns_to_check_in=["work_main_job_title", "work_main_job_role"],
        positive_regex_pattern=childcare_pattern.positive_regex_pattern,
        negative_regex_pattern=childcare_pattern.negative_regex_pattern,
    )

    age_under_16 = F.col("age_at_visit") < F.lit(16)
    age_over_four = F.col("age_at_visit") > F.lit(4)

    # Rule_id: 6000
    update_work_status_student_v0 = (
        (school_regex_hit & flag_records_for_school_v2_rules())
        | (university_regex_hit & flag_records_for_uni_v0_rules())
        | (college_regex_hit & flag_records_for_college_v0_rules())
        | (age_over_four & age_under_16)
    )

    # Rule_id: 6001
    update_work_status_student_v0_a = (childcare_regex_hit & flag_records_for_childcare_v0_rules()) | (
        school_regex_hit & flag_records_for_childcare_v0_rules()
    )

    # Rule_id: 6002
    update_work_status_student_v1_a = (
        (school_regex_hit & flag_records_for_school_v2_rules())
        | (university_regex_hit & flag_records_for_uni_v1_rules())
        | (college_regex_hit & flag_records_for_college_v1_rules())
        | (age_over_four & age_under_16)
    )

    # Rule_id: 6003
    update_work_status_student_v1_c = (childcare_regex_hit & flag_records_for_childcare_v1_rules()) | (
        school_regex_hit & flag_records_for_childcare_v1_rules()
    )

    # Rule_id: 6004
    update_work_status_student_v2_e = (childcare_regex_hit & flag_records_for_childcare_v2_b_rules()) | (
        school_regex_hit & flag_records_for_childcare_v2_b_rules()
    )

    # Rule_id: 6005
    update_work_status_student_v2_a = (school_regex_hit & flag_records_for_school_v2_rules()) | (
        age_over_four & age_under_16
    )

    # Rule_id: 6006
    update_work_status_student_v2_b = college_regex_hit & flag_records_for_college_v2_rules()

    # Rule_id: 6007
    update_work_status_student_v2_c = university_regex_hit & flag_records_for_uni_v2_rules()

    # Rule_id: 6008
    update_work_location_general = flag_records_for_work_location_null() | flag_records_for_work_location_student()

    # Please note the order of *_edited columns, these must come before the in-place updates

    # first start by taking a copy of the original work variables
    _df = (
        df.withColumn("work_location_original", F.col("work_location"))
        .withColumn("work_status_v0_original", F.col("work_status_v0"))
        .withColumn("work_status_v1_original", F.col("work_status_v1"))
        .withColumn("work_status_v2_original", F.col("work_status_v2"))
        .withColumn(
            "work_location",
            F.when(update_work_location, F.lit("Working from home")).otherwise(F.col("work_location")),
        )
        .withColumn(
            "work_status_v0",
            F.when(update_work_status_self_employed_v0, F.lit("Self-employed")).otherwise(F.col("work_status_v0")),
        )
        .withColumn(
            "work_status_v1",
            F.when(update_work_status_self_employed_v1_a, F.lit("Self-employed and currently working")).otherwise(
                F.col("work_status_v1")
            ),
        )
        .withColumn(
            "work_status_v1",
            F.when(update_work_status_self_employed_v1_b, F.lit("Self-employed and currently not working")).otherwise(
                F.col("work_status_v1")
            ),
        )
        .withColumn(
            "work_status_v2",
            F.when(
                update_work_status_self_employed_v2_a,
                F.lit("Self-employed and currently working"),
            ).otherwise(F.col("work_status_v2")),
        )
        .withColumn(
            "work_status_v2",
            F.when(update_work_status_self_employed_v2_b, F.lit("Self-employed and currently not working")).otherwise(
                F.col("work_status_v2")
            ),
        )
    )

    _df2 = spark_session.createDataFrame(_df.rdd, schema=_df.schema)  # breaks lineage to avoid Java OOM Error

    _df3 = (
        _df2.withColumn(
            "work_status_v0",
            F.when(update_work_status_student_v0 | update_work_status_student_v0_a, F.lit("Student")).otherwise(
                F.col("work_status_v0")
            ),
        )
        .withColumn(
            "work_status_v1",
            F.when(update_work_status_student_v1_a, F.lit("5y and older in full-time education")).otherwise(
                F.col("work_status_v1")
            ),
        )
        .withColumn(
            "work_status_v1",
            F.when(update_work_status_student_v1_c, F.lit("Child under 5y attending child care")).otherwise(
                F.col("work_status_v1")
            ),
        )
        .withColumn(
            "work_status_v2",
            F.when(update_work_status_student_v2_a, F.lit("4-5y and older at school/home-school")).otherwise(
                F.col("work_status_v2")
            ),
        )
        .withColumn(
            "work_status_v2",
            F.when(
                update_work_status_student_v2_b, F.lit("Attending college or FE (including if temporarily absent)")
            ).otherwise(F.col("work_status_v2")),
        )
        .withColumn(
            "work_status_v2",
            F.when(
                update_work_status_student_v2_c, F.lit("Attending university (including if temporarily absent)")
            ).otherwise(F.col("work_status_v2")),
        )
        .withColumn(
            "work_status_v2",
            F.when(update_work_status_student_v2_e, F.lit("Child under 4-5y attending child care")).otherwise(
                F.col("work_status_v2")
            ),
        )
        .withColumn(
            "work_status_v0",
            F.when(
                update_work_status_retired, F.lit("Not working (unemployed, retired, long-term sick etc.)")
            ).otherwise(F.col("work_status_v0")),
        )
        .withColumn(
            "work_status_v1",
            F.when(update_work_status_retired, F.lit("Retired")).otherwise(F.col("work_status_v1")),
        )
        .withColumn(
            "work_status_v2",
            F.when(update_work_status_retired, F.lit("Retired")).otherwise(F.col("work_status_v2")),
        )
    )

    _df4 = spark_session.createDataFrame(_df3.rdd, schema=_df3.schema)  # breaks lineage to avoid Java OOM Error

    _df5 = (
        _df4.withColumn(
            "work_status_v0",
            F.when(
                update_work_status_not_working_v0, F.lit("Not working (unemployed, retired, long-term sick etc.)")
            ).otherwise(F.col("work_status_v0")),
        )
        .withColumn(
            "work_status_v1",
            F.when(update_work_status_not_working_v1_a, F.lit("Employed and currently not working")).otherwise(
                F.col("work_status_v1")
            ),
        )
        .withColumn(
            "work_status_v1",
            F.when(update_work_status_not_working_v1_b, F.lit("Self-employed and currently not working")).otherwise(
                F.col("work_status_v1")
            ),
        )
        .withColumn(
            "work_status_v2",
            F.when(update_work_status_not_working_v2_a, F.lit("Employed and currently not working")).otherwise(
                F.col("work_status_v2")
            ),
        )
        .withColumn(
            "work_status_v2",
            F.when(update_work_status_not_working_v2_b, F.lit("Self-employed and currently not working")).otherwise(
                F.col("work_status_v2")
            ),
        )
        .withColumn(
            "work_status_v0",
            F.when(update_work_status_furlough_v0, F.lit("Furloughed (temporarily not working)")).otherwise(
                F.col("work_status_v0")
            ),
        )
        .withColumn(
            "work_status_v1",
            F.when(update_work_status_furlough_v1_a, F.lit("Employed and currently not working")).otherwise(
                F.col("work_status_v1")
            ),
        )
        .withColumn(
            "work_status_v1",
            F.when(update_work_status_furlough_v1_b, F.lit("Self-employed and currently not working")).otherwise(
                F.col("work_status_v1")
            ),
        )
        .withColumn(
            "work_status_v2",
            F.when(update_work_status_furlough_v2_a, F.lit("Employed and currently not working")).otherwise(
                F.col("work_status_v2")
            ),
        )
        .withColumn(
            "work_status_v2",
            F.when(update_work_status_furlough_v2_b, F.lit("Self-employed and currently not working")).otherwise(
                F.col("work_status_v2")
            ),
        )
        .withColumn(
            "work_location",
            F.when(
                update_work_location_general,
                F.lit("Not applicable, not currently working"),
            ).otherwise(F.col("work_location")),
        )
    )

    if drop_original_variables:
        # replace original versions with their cleaned versions
        _df5 = _df5.drop(
            "work_location_original",
            "work_status_v0_original",
            "work_status_v1_original",
            "work_status_v2_original",
        )

    return _df5


def fix_timestamps(df: DataFrame):
    """
    Fix any issues with dates saved in timestamp format drifting ahead by n hours.
    """
    date_cols = [c for c in df.columns if "date" in c and "datetime" not in c]
    d_types_list = [list(d) for d in df.select(*date_cols).dtypes]
    d_types = {d[0]: d[1] for d in d_types_list}
    for col in date_cols:
        if d_types[col] == "timestamp":
            df = df.withColumn(col, F.date_format(F.col(col), "yyyy-MM-dd"))
    return df


def flag_records_to_reclassify(df: DataFrame) -> DataFrame:
    """
    Adds various flags to indicate which rules were triggered for a given record.
    TODO: Don't use this function - it is not up to date with derive module
    """
    # Work from Home rules
    df = df.withColumn("wfh_rules", flag_records_for_work_from_home_rules())

    # Furlough rules
    df = df.withColumn("furlough_rules_v0", flag_records_for_furlough_rules_v0())

    df = df.withColumn("furlough_rules_v1_a", flag_records_for_furlough_rules_v1_a())

    df = df.withColumn("furlough_rules_v1_b", flag_records_for_furlough_rules_v1_b())

    df = df.withColumn("furlough_rules_v2_a", flag_records_for_furlough_rules_v2_a())

    df = df.withColumn("furlough_rules_v2_b", flag_records_for_furlough_rules_v2_b())

    # Self-employed rules
    df = df.withColumn("self_employed_rules_v1_a", flag_records_for_self_employed_rules_v1_a())

    df = df.withColumn("self_employed_rules_v1_b", flag_records_for_self_employed_rules_v1_b())

    df = df.withColumn("self_employed_rules_v2_a", flag_records_for_self_employed_rules_v2_a())

    df = df.withColumn("self_employed_rules_v2_b", flag_records_for_self_employed_rules_v2_b())

    # Retired rules
    df = df.withColumn("retired_rules_generic", flag_records_for_retired_rules())

    # Not-working rules
    df = df.withColumn("not_working_rules_v0", flag_records_for_not_working_rules_v0())

    df = df.withColumn("not_working_rules_v1_a", flag_records_for_not_working_rules_v1_a())

    df = df.withColumn("not_working_rules_v1_b", flag_records_for_not_working_rules_v1_b())

    df = df.withColumn("not_working_rules_v2_a", flag_records_for_not_working_rules_v2_a())

    df = df.withColumn("not_working_rules_v2_b", flag_records_for_not_working_rules_v2_b())

    # Student rules
    # df = df.withColumn("student_rules_v0", flag_records_for_student_v0_rules())

    # df = df.withColumn("student_rules_v1", flag_records_for_student_v1_rules())

    df = df.withColumn("school_rules_v2", flag_records_for_school_v2_rules())

    # University rules
    df = df.withColumn("uni_rules_v2", flag_records_for_uni_v2_rules())

    df = df.withColumn("college_rules_v2", flag_records_for_college_v2_rules())

    return df


def get_differences(base_df: DataFrame, compare_df: DataFrame, unique_id_column: str, diff_sample_size: int = 10):
    window = Window.partitionBy("column_name").orderBy("column_name")
    cols_to_check = [col for col in base_df.columns if col in compare_df.columns and col != unique_id_column]

    for col in cols_to_check:
        base_df = base_df.withColumnRenamed(col, f"{col}_ref")

    df = base_df.join(compare_df, on=unique_id_column, how="left")

    diffs_df = df.select(
        [
            F.when(F.col(col).eqNullSafe(F.col(f"{col}_ref")), None).otherwise(F.col(unique_id_column)).alias(col)
            for col in cols_to_check
        ]
    )
    diffs_df = diffs_df.select(
        F.explode(
            F.array(
                [
                    F.struct(F.lit(col).alias("column_name"), F.col(col).alias(unique_id_column))
                    for col in diffs_df.columns
                ]
            )
        ).alias("kvs")
    )
    diffs_df = (
        diffs_df.select("kvs.column_name", f"kvs.{unique_id_column}")
        .filter(F.col(unique_id_column).isNotNull())
        .withColumn("ROW", F.row_number().over(window))
        .filter(F.col("ROW") < diff_sample_size)
    ).drop("ROW")

    counts_df = df.select(
        *[
            F.sum(F.when(F.col(c).eqNullSafe(F.col(f"{c}_ref")), 0).otherwise(1)).alias(c).cast("integer")
            for c in cols_to_check
        ],
        *[
            F.sum(F.when((~F.col(c).eqNullSafe(F.col(f"{c}_ref"))) & (F.col(f"{c}_ref").isNotNull()), 1).otherwise(0))
            .alias(f"{c}_non_improved")
            .cast("integer")
            for c in cols_to_check
        ],
    )
    counts_df = counts_df.select(
        F.explode(
            F.array(
                [
                    F.struct(
                        F.lit(col).alias("column_name"),
                        F.col(col).alias("difference_count"),
                        F.col(f"{col}_non_improved").alias("difference_count_non_improved"),
                    )
                    for col in [c for c in counts_df.columns if not c.endswith("_non_improved")]
                ]
            )
        ).alias("kvs")
    )
    counts_df = counts_df.select("kvs.column_name", "kvs.difference_count", "kvs.difference_count_non_improved")
    return counts_df, diffs_df
