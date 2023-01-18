# flake8: noqa
from functools import reduce
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
from cishouseholds.derive import assign_regex_from_map
from cishouseholds.derive import assign_regex_match_result
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
from cishouseholds.edit import rename_column_names
from cishouseholds.edit import replace_sample_barcode
from cishouseholds.edit import update_column_values_from_map
from cishouseholds.edit import update_face_covering_outside_of_home
from cishouseholds.edit import update_person_count_from_ages
from cishouseholds.edit import update_think_have_covid_symptom_any
from cishouseholds.edit import update_to_value_if_any_not_null
from cishouseholds.expressions import any_column_equal_value
from cishouseholds.expressions import array_contains_any
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
from cishouseholds.pipeline.high_level_transformations import create_formatted_datetime_string_columns
from cishouseholds.pipeline.mapping import column_name_maps
from cishouseholds.pipeline.mapping import date_cols_min_date_dict
from cishouseholds.regex.healthcare_regex import healthcare_classification
from cishouseholds.regex.healthcare_regex import patient_facing_classification
from cishouseholds.regex.healthcare_regex import patient_facing_pattern
from cishouseholds.regex.healthcare_regex import priority_map
from cishouseholds.regex.healthcare_regex import roles_map
from cishouseholds.regex.healthcare_regex import social_care_classification
from cishouseholds.regex.regex_patterns import at_school_pattern
from cishouseholds.regex.regex_patterns import at_university_pattern
from cishouseholds.regex.regex_patterns import childcare_pattern
from cishouseholds.regex.regex_patterns import furloughed_pattern
from cishouseholds.regex.regex_patterns import in_college_or_further_education_pattern
from cishouseholds.regex.regex_patterns import not_working_pattern
from cishouseholds.regex.regex_patterns import retired_regex_pattern
from cishouseholds.regex.regex_patterns import self_employed_regex
from cishouseholds.regex.regex_patterns import work_from_home_pattern


def nims_transformations(df: DataFrame) -> DataFrame:
    """Clean and transform NIMS data after reading from table."""
    df = rename_column_names(df, column_name_maps["nims_column_name_map"])
    df = assign_column_to_date_string(df, "nims_vaccine_dose_1_date", reference_column="nims_vaccine_dose_1_datetime")
    df = assign_column_to_date_string(df, "nims_vaccine_dose_2_date", reference_column="nims_vaccine_dose_2_datetime")

    # TODO: Derive nims_linkage_status, nims_vaccine_classification, nims_vaccine_dose_1_time, nims_vaccine_dose_2_time
    return df


def blood_past_positive_transformations(df: DataFrame) -> DataFrame:
    """Run required post-join transformations for blood_past_positive"""
    df = df.withColumn("blood_past_positive_flag", F.when(F.col("blood_past_positive").isNull(), 0).otherwise(1))
    return df


def design_weights_lookup_transformations(df: DataFrame) -> DataFrame:
    """Selects only required fields from the design_weight_lookup"""
    design_weight_columns = ["scaled_design_weight_swab_non_adjusted", "scaled_design_weight_antibodies_non_adjusted"]
    df = df.select(*design_weight_columns, "ons_household_id")
    return df


def replace_design_weights_transformations(df: DataFrame) -> DataFrame:
    """Run required post-join transformations for replace_design_weights"""
    df = df.withColumn(
        "local_authority_unity_authority_code",
        F.when(F.col("local_authority_unity_authority_code") == "E06000062", "E07000154")
        .when(F.col("local_authority_unity_authority_code") == "E06000061", "E07000156")
        .otherwise(F.col("local_authority_unity_authority_code")),
    )
    df = df.withColumn(
        "region_code",
        F.when(F.col("region_code") == "W92000004", "W99999999")
        .when(F.col("region_code") == "S92000003", "S99999999")
        .when(F.col("region_code") == "N92000002", "N99999999")
        .otherwise(F.col("region_code")),
    )
    return df


def derive_overall_vaccination(df: DataFrame) -> DataFrame:
    """Derive overall vaccination status from NIMS and CIS data."""
    return df


def ordered_household_id_tranformations(df: DataFrame) -> DataFrame:
    """Read in a survey responses table and join it onto the participants extract to ensure matching ordered household ids"""
    join_on_columns = ["ons_household_id", "ordered_household_id"]
    df = df.select(join_on_columns).distinct()
    return df


def add_pattern_matching_flags(df: DataFrame) -> DataFrame:
    """Add result of various regex pattern matchings"""
    # df = df.drop(
    #     "work_health_care_patient_facing_original",
    #     "work_social_care_original",
    #     "work_care_nursing_home_original",
    #     "work_direct_contact_patients_or_clients_original",
    # )

    df = df.withColumn("work_main_job_title", F.upper(F.col("work_main_job_title")))
    df = df.withColumn("work_main_job_role", F.upper(F.col("work_main_job_role")))

    df = assign_regex_from_map(
        df=df,
        column_name_to_assign="regex_derived_job_sector",
        reference_columns=["work_main_job_title", "work_main_job_role"],
        map=roles_map,
        priority_map=priority_map,
    )
    # create healthcare area flag
    df = df.withColumn("work_health_care_area", F.lit(None))
    for healthcare_type, roles in healthcare_classification.items():  # type: ignore
        df = df.withColumn(
            "work_health_care_area",
            F.when(array_contains_any("regex_derived_job_sector", roles), healthcare_type).otherwise(
                F.col("work_health_care_area")
            ),  # type: ignore
        )
    # TODO: need to exclude healthcare types from social care matching
    df = df.withColumn("work_social_care_area", F.lit(None))
    for social_care_type, roles in social_care_classification.items():  # type: ignore
        df = df.withColumn(
            "work_social_care_area",
            F.when(F.col("work_health_care_area").isNotNull(), None)
            .when(array_contains_any("regex_derived_job_sector", roles), social_care_type)
            .otherwise(F.col("work_social_care_area")),  # type: ignore
        )

    # add boolean flags for working in healthcare or socialcare

    df = df.withColumn("works_health_care", F.when(F.col("work_health_care_area").isNotNull(), "Yes").otherwise("No"))

    df = assign_regex_match_result(
        df=df,
        columns_to_check_in=["work_main_job_title", "work_main_job_role"],
        column_name_to_assign="work_direct_contact_patients_or_clients_regex_derived",
        positive_regex_pattern=patient_facing_pattern.positive_regex_pattern,
        negative_regex_pattern=patient_facing_pattern.negative_regex_pattern,
    )
    df = df.withColumn(
        "work_direct_contact_patients_or_clients",
        F.when(
            (F.col("work_health_care_area_original") == F.col("work_health_care_area"))
            & (F.col("work_direct_contact_patients_or_clients").isNotNull()),
            F.col("work_direct_contact_patients_or_clients"),
        )
        .when(
            (
                (F.col("works_health_care") == "Yes")
                | (F.col("work_direct_contact_patients_or_clients_regex_derived") == True)
            )
            & (~array_contains_any("regex_derived_job_sector", patient_facing_classification["N"])),
            "Yes",
        )
        .otherwise("No"),
    )
    df = assign_column_value_from_multiple_column_map(
        df,
        "work_health_care_patient_facing",
        [
            ["No", ["No", None]],
            ["No", ["Yes", None]],
            ["Yes, primary care, patient-facing", ["Yes", "Primary"]],
            ["Yes, secondary care, patient-facing", ["Yes", "Secondary"]],
            ["Yes, other healthcare, patient-facing", ["Yes", "Other"]],
            ["Yes, primary care, non-patient-facing", ["No", "Primary"]],
            ["Yes, secondary care, non-patient-facing", ["No", "Secondary"]],
            ["Yes, other healthcare, non-patient-facing", ["No", "Other"]],
        ],
        ["work_direct_contact_patients_or_clients", "work_health_care_area"],
    )
    df = assign_column_value_from_multiple_column_map(
        df,
        "work_social_care",
        [
            ["No", ["No", None]],
            ["No", ["Yes", None]],
            ["Yes, care/residential home, resident-facing", ["Yes", "Care/Residential home"]],
            ["Yes, other social care, resident-facing", ["Yes", "Other"]],
            ["Yes, care/residential home, non-resident-facing", ["No", "Care/Residential home"]],
            ["Yes, other social care, non-resident-facing", ["No", "Other"]],
        ],
        ["work_direct_contact_patients_or_clients", "work_social_care_area"],
    )
    df = assign_column_value_from_multiple_column_map(
        df,
        "work_patient_facing_clean",
        [["Yes", ["Yes", "Yes"]], ["No", ["No", "Yes"]], ["Not working in health care", ["No", "No"]]],
        ["work_direct_contact_patients_or_clients", "works_health_care"],
    )
    # work_status_columns = [col for col in df.columns if "work_status_" in col]
    # for work_status_column in work_status_columns:
    #     df = df.withColumn(
    #         work_status_column,
    #         F.when(F.col("not_working"), "not working")
    #         .when(F.col("at_school") | F.col("at_university"), "student")
    #         .when(F.array_contains(F.col("regex_derived_job_sector"), "apprentice"), "working")
    #         .otherwise(F.col(work_status_column)),
    #     )
    return df.select(
        "work_main_job_title",
        "work_main_job_role",
        "work_direct_contact_patients_or_clients",
        "work_social_care_area",
        "work_health_care_area",
        "work_health_care_patient_facing",
        "work_patient_facing_clean",
        "work_social_care",
        "works_health_care",
    )


def transform_from_lookups(
    df: DataFrame, cohort_lookup: DataFrame, travel_countries_lookup: DataFrame, tenure_group: DataFrame
):
    cohort_lookup = cohort_lookup.withColumnRenamed("participant_id", "cohort_participant_id")
    df = df.join(
        F.broadcast(cohort_lookup),
        how="left",
        on=((df.participant_id == cohort_lookup.cohort_participant_id) & (df.study_cohort == cohort_lookup.old_cohort)),
    ).drop("cohort_participant_id")
    df = df.withColumn("study_cohort", F.coalesce(F.col("new_cohort"), F.col("study_cohort"))).drop(
        "new_cohort", "old_cohort"
    )
    df = df.join(
        F.broadcast(travel_countries_lookup.withColumn("REPLACE_COUNTRY", F.lit(True))),
        how="left",
        on=df.been_outside_uk_last_country == travel_countries_lookup.been_outside_uk_last_country_old,
    )
    df = df.withColumn(
        "been_outside_uk_last_country",
        F.when(F.col("REPLACE_COUNTRY"), F.col("been_outside_uk_last_country_new")).otherwise(
            F.col("been_outside_uk_last_country"),
        ),
    ).drop("been_outside_uk_last_country_old", "been_outside_uk_last_country_new", "REPLACE_COUNTRY")

    for key, value in column_name_maps["tenure_group_variable_map"].items():
        tenure_group = tenure_group.withColumnRenamed(key, value)

    df = df.join(tenure_group, on=(df["ons_household_id"] == tenure_group["UAC"]), how="left").drop("UAC")
    return df


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
            "other_covid_infection_test_result",
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
            "think_have_covid_symptom_chest_pain",
            "think_have_covid_symptom_palpitations",
            "think_have_covid_symptom_vertigo_or_dizziness",
            "think_have_covid_symptom_anxiety",
            "think_have_covid_symptom_low_mood",
            "think_have_covid_symptom_memory_loss_or_confusion",
            "think_have_covid_symptom_difficulty_concentrating",
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

    # df = fill_forward_from_last_change(
    #     df=df,
    #     fill_forward_columns=[
    #         "cis_covid_vaccine_date",
    #         "cis_covid_vaccine_number_of_doses",
    #         "cis_covid_vaccine_type",
    #         "cis_covid_vaccine_type_other",
    #         "cis_covid_vaccine_received",
    #     ],
    #     participant_id_column="participant_id",
    #     visit_datetime_column="visit_datetime",
    #     record_changed_column="cis_covid_vaccine_received",
    #     record_changed_value="Yes",
    # )
    df = create_formatted_datetime_string_columns(df)

    df = clean_covid_test_swab(df)
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
