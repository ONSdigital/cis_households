# flake8: noqa
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.dataframe import DataFrame

from cishouseholds.derive import assign_age_at_date
from cishouseholds.derive import assign_column_from_mapped_list_key
from cishouseholds.derive import assign_column_given_proportion
from cishouseholds.derive import assign_column_regex_match
from cishouseholds.derive import assign_column_to_date_string
from cishouseholds.derive import assign_column_uniform_value
from cishouseholds.derive import assign_column_value_from_multiple_column_map
from cishouseholds.derive import assign_consent_code
from cishouseholds.derive import assign_date_difference
from cishouseholds.derive import assign_date_from_filename
from cishouseholds.derive import assign_datetime_from_coalesced_columns_and_log_source
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
from cishouseholds.derive import assign_substring
from cishouseholds.derive import assign_taken_column
from cishouseholds.derive import assign_test_target
from cishouseholds.derive import assign_true_if_any
from cishouseholds.derive import assign_unique_id_column
from cishouseholds.derive import assign_visit_order
from cishouseholds.derive import assign_work_health_care
from cishouseholds.derive import assign_work_patient_facing_now
from cishouseholds.derive import assign_work_person_facing_now
from cishouseholds.derive import assign_work_social_column
from cishouseholds.derive import assign_work_status_group
from cishouseholds.derive import concat_fields_if_true
from cishouseholds.derive import contact_known_or_suspected_covid_type
from cishouseholds.derive import count_value_occurrences_in_column_subset_row_wise
from cishouseholds.derive import derive_cq_pattern
from cishouseholds.derive import derive_had_symptom_last_7days_from_digital
from cishouseholds.derive import derive_household_been_columns
from cishouseholds.derive import map_options_to_bool_columns
from cishouseholds.derive import mean_across_columns
from cishouseholds.edit import apply_value_map_multiple_columns
from cishouseholds.edit import assign_from_map
from cishouseholds.edit import clean_barcode
from cishouseholds.edit import clean_barcode_simple
from cishouseholds.edit import clean_postcode
from cishouseholds.edit import clean_within_range
from cishouseholds.edit import convert_null_if_not_in_list
from cishouseholds.edit import edit_to_sum_or_max_value
from cishouseholds.edit import format_string_upper_and_clean
from cishouseholds.edit import map_column_values_to_null
from cishouseholds.edit import rename_column_names
from cishouseholds.edit import update_column_if_ref_in_list
from cishouseholds.edit import update_column_in_time_window
from cishouseholds.edit import update_column_values_from_map
from cishouseholds.edit import update_face_covering_outside_of_home
from cishouseholds.edit import update_person_count_from_ages
from cishouseholds.edit import update_strings_to_sentence_case
from cishouseholds.edit import update_think_have_covid_symptom_any
from cishouseholds.edit import update_to_value_if_any_not_null
from cishouseholds.edit import update_work_facing_now_column
from cishouseholds.expressions import sum_within_row
from cishouseholds.impute import fill_backwards_overriding_not_nulls
from cishouseholds.impute import fill_backwards_work_status_v2
from cishouseholds.impute import fill_forward_from_last_change
from cishouseholds.impute import fill_forward_only_to_nulls
from cishouseholds.impute import fill_forward_only_to_nulls_in_dataset_based_on_column
from cishouseholds.impute import impute_and_flag
from cishouseholds.impute import impute_by_distribution
from cishouseholds.impute import impute_by_k_nearest_neighbours
from cishouseholds.impute import impute_by_mode
from cishouseholds.impute import impute_by_ordered_fill_forward
from cishouseholds.impute import impute_latest_date_flag
from cishouseholds.impute import impute_outside_uk_columns
from cishouseholds.impute import impute_visit_datetime
from cishouseholds.impute import merge_previous_imputed_values
from cishouseholds.mapping import column_name_maps
from cishouseholds.pipeline.timestamp_map import cis_digital_datetime_map
from cishouseholds.pyspark_utils import get_or_create_spark_session
from cishouseholds.validate_class import SparkValidate


def transform_blood_delta(df: DataFrame) -> DataFrame:
    """
    Call functions to process input for blood deltas.
    """
    df = assign_test_target(df, "antibody_test_target", "blood_test_source_file")
    df = assign_substring(
        df,
        column_name_to_assign="antibody_test_plate_common_id",
        column_to_substring="antibody_test_plate_id",
        start_position=5,
        substring_length=5,
    )
    df = assign_unique_id_column(
        df=df,
        column_name_to_assign="unique_antibody_test_id",
        concat_columns=["blood_sample_barcode", "antibody_test_plate_common_id", "antibody_test_well_id"],
    )
    df = clean_barcode(
        df=df, barcode_column="blood_sample_barcode", edited_column="blood_sample_barcode_edited_in_bloods_dataset_flag"
    )
    return df


def add_historical_fields(df: DataFrame):
    """
    Add empty values for union with historical data. Also adds constant
    values for continuation with historical data.
    """
    historical_columns = {
        "siemens_antibody_test_result_classification": "string",
        "siemens_antibody_test_result_value": "float",
        "antibody_test_tdi_result_value": "float",
        "lims_id": "string",
        "plate_storage_method": "string",
    }
    for column, type in historical_columns.items():
        if column not in df.columns:
            df = df.withColumn(column, F.lit(None).cast(type))
    if "antibody_assay_category" not in df.columns:
        df = assign_column_uniform_value(df, "antibody_assay_category", "Post 2021-03-01")
    df = df.select(sorted(df.columns))
    return df


def add_fields(df: DataFrame):
    """Add fields that might be missing in example data."""
    new_columns = {
        "antibody_test_undiluted_result_value": "float",
        "antibody_test_bounded_result_value": "float",
    }
    for column, type in new_columns.items():
        if column not in df.columns:
            df = df.withColumn(column, F.lit(None).cast(type))
    df = df.select(sorted(df.columns))
    return df


def transform_swab_delta(df: DataFrame) -> DataFrame:
    """
    Transform swab delta - derive new fields that do not depend on merging with survey responses.
    """
    spark_session = get_or_create_spark_session()
    df = clean_barcode(
        df=df, barcode_column="swab_sample_barcode", edited_column="swab_sample_barcode_edited_in_swab_dataset_flag"
    )
    df = assign_column_to_date_string(df, "pcr_result_recorded_date_string", "pcr_result_recorded_datetime")
    df = derive_cq_pattern(
        df, ["orf1ab_gene_pcr_cq_value", "n_gene_pcr_cq_value", "s_gene_pcr_cq_value"], spark_session
    )
    df = assign_unique_id_column(
        df, "unique_pcr_test_id", ["swab_sample_barcode", "pcr_result_recorded_datetime", "cq_pattern"]
    )

    df = mean_across_columns(
        df, "mean_pcr_cq_value", ["orf1ab_gene_pcr_cq_value", "n_gene_pcr_cq_value", "s_gene_pcr_cq_value"]
    )
    df = assign_isin_list(
        df=df,
        column_name_to_assign="one_positive_pcr_target_only",
        reference_column="cq_pattern",
        values_list=["N only", "OR only", "S only"],
        true_false_values=[1, 0],
    )
    return df


def transform_swab_delta_testKit(df: DataFrame):
    df = df.drop("testKit")

    return df


def transform_survey_responses_version_0_delta(df: DataFrame) -> DataFrame:
    """
    Call functions to process input for iqvia version 0 survey deltas.
    """
    df = assign_taken_column(df=df, column_name_to_assign="swab_taken", reference_column="swab_sample_barcode")
    df = assign_taken_column(df=df, column_name_to_assign="blood_taken", reference_column="blood_sample_barcode")

    df = assign_column_uniform_value(df, "survey_response_dataset_major_version", 0)
    df = df.withColumn("sex", F.coalesce(F.col("sex"), F.col("gender"))).drop("gender")

    df = map_column_values_to_null(
        df=df,
        value="Participant Would Not/Could Not Answer",
        column_list=[
            "ethnicity",
            "work_status_v0",
            "work_location",
            "survey_response_type",
            "participant_withdrawal_reason",
            "work_not_from_home_days_per_week",
        ],
    )

    column_editing_map = {
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
            "ability_to_socially_distance_at_work_or_education",
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
    df = df.drop(
        "cis_covid_vaccine_date",
        "cis_covid_vaccine_number_of_doses",
        "cis_covid_vaccine_type",
        "cis_covid_vaccine_type_other",
        "cis_covid_vaccine_received",
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


def pre_generic_digital_transformations(df: DataFrame) -> DataFrame:
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
        ordered_columns=[
            "swab_taken_datetime",
            "blood_taken_datetime",
            "survey_completed_datetime",
            "survey_last_modified_datetime",
            "swab_return_date",
            "blood_return_date",
            "swab_return_future_date",
            "blood_return_future_date",
        ],
        date_format="yyyy-MM-dd",
        time_format="HH:mm:ss",
        file_date_column="file_date",
        min_date="2022/05/01",
        default_timestamp="12:00:00",
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
        "Don't Know": None,
        "I don't know the type": "Don't know type",
        "Dont know": None,
        "Don&#39;t know": None,
    }
    df = apply_value_map_multiple_columns(
        df,
        {k: dont_know_mapping_dict for k in dont_know_columns},
    )

    df = df.withColumn("self_isolating_reason_digital", F.col("self_isolating_reason"))

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
                        "I haven't had any symptoms but I live with someone who has or has had symptoms or a positive test",  # noqa: E501
                        "I haven&#39;t had any symptoms but I live with someone who has or has had symptoms or a positive test",  # noqa: E501
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
        ["self_isolating", "self_isolating_reason_digital"],
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
                        "Currently not working -  for example on sick or other leave such as maternity or paternity for longer than 4 weeks",  # noqa: E501
                        "Or currently not working -  for example on sick or other leave such as maternity or paternity for longer than 4 weeks?",  # noqa: E501
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
                        "Currently not working -  for example on sick or other leave such as maternity or paternity for longer than 4 weeks",  # noqa: E501
                        "Or currently not working -  for example on sick or other leave such as maternity or paternity for longer than 4 weeks?",  # noqa: E501
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
                    "Not looking for paid work. This includes looking after the home or family or not wanting a job or being long-term sick or disabled",  # noqa: E501
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
                    [
                        "Currently not working -  for example on sick or other leave such as maternity or paternity for longer than 4 weeks",  # noqa: E501
                        "Or currently not working -  for example on sick or other leave such as maternity or paternity for longer than 4 weeks?",
                    ],  # noqa: E501
                    None,
                    None,
                ],
            ],
            [
                "Self-employed and currently working",
                [
                    "self-employed"
                    "Currently working. This includes if you are on sick or other leave for less than 4 weeks",
                    None,
                    None,
                ],
            ],
            [
                "Self-employed and currently not working",
                [
                    "self-employed"
                    "Currently not working. This includes if you are on sick or other leave such as maternity or paternity for longer than 4 weeks",
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
                    "A child below school age and attending a nursery or pre-school or childminder",
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
                    "Currently not working. This includes if you are on sick or other leave such as maternity or paternity for longer than 4 weeks",  # noqa: E501
                    None,
                    None,
                ],
            ],
            ["Employed", ["Self-employed", None, "Looking for paid work and able to start", None]],
            [
                "Self-employed",
                [
                    "Self-employed",
                    None,
                    "Not looking for paid work. This includes looking after the home or family or not wanting a job or being long-term sick or disabled",  # noqa: E501
                    None,
                ],
            ],
            ["Self-employed", [None, None, None]],
            ["Not working (unemployed, retired, long-term sick etc.)", ["Self-employed", None, None, None]],
            [
                "Not working (unemployed, retired, long-term sick etc.)",
                [
                    "Not in paid work. This includes being unemployed or doing voluntary work",
                    None,
                    "Looking for paid work and able to start",
                    None,
                ],
            ],
            [
                "Not working (unemployed, retired, long-term sick etc.)",
                [
                    "Not in paid work. This includes being unemployed or doing voluntary work",
                    None,
                    "Not looking for paid work. This includes looking after the home or family or not wanting a job or being long-term sick or disabled",  # noqa: E501
                    None,
                ],
            ],
            [
                "Not working (unemployed, retired, long-term sick etc.)",
                ["Not in paid work. This includes being unemployed or doing voluntary work", None, "Retired", None],
            ],
            [
                "Not working (unemployed, retired, long-term sick etc.)",
                ["Not in paid work. This includes being unemployed or doing voluntary work", None, None, None],
            ],
            [
                "Student",
                [
                    ["In education", None],
                    None,
                    None,
                    "A child below school age and not attending a nursery or pre-school or childminder",
                ],
            ],
            [
                "Student",
                [
                    ["In education", None],
                    None,
                    None,
                    "A child below school age and attending a nursery or pre-school or childminder",
                ],
            ],
            ["Student", [["In education", None], None, None, "A child aged 4 or over at school"]],
            ["Student", [["In education", None], None, None, "A child aged 4 or over at home-school"]],
            [
                "Student",
                [
                    ["In education", None],
                    None,
                    None,
                    "Attending a college or other further education provider including apprenticeships",
                ],
            ],
        ],
        column_list,
    )
    df = clean_barcode_simple(df, "swab_sample_barcode_user_entered")
    df = clean_barcode_simple(df, "blood_sample_barcode_user_entered")
    df = map_options_to_bool_columns(
        df,
        "currently_smokes_or_vapes_description",
        {
            "cigarettes": "smoke_cigarettes",
            "cigars": "smokes_cigar",
            "pipe": "smokes_pipe",
            "vape/E-cigarettes": "smokes_vape_e_cigarettes",
            "Hookah/shisha pipes": "smokes_hookah_shisha_pipes",
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
        "cis_covid_vaccine_number_of_doses",
        "cis_covid_vaccine_type_1",
        "cis_covid_vaccine_type_2",
        "cis_covid_vaccine_type_3",
        "cis_covid_vaccine_type_4",
        "cis_covid_vaccine_type_5",
        "cis_covid_vaccine_type_6",
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
            "Primary care - for example in a GP or dentist": "Yes, in primary care, e.g. GP, dentist",
            "Secondary care - for example in a hospital": "Yes, in secondary care, e.g. hospital",
            "Another type of healthcare - for example mental health services": "Yes, in other healthcare settings, e.g. mental health",  # noqa: E501
        },
        "illness_reduces_activity_or_ability": {
            "Yes a little": "Yes, a little",
            "Yes a lot": "Yes, a lot",
        },
        "work_location": {
            "From home meaning in the same grounds or building as your home": "Working from home",
            "Somewhere else meaning not at your home)": "Working somewhere else (not your home)",
            "Both from home and work somewhere else": "Both (from home and somewhere else)",
        },
        "transport_to_work_or_education": {
            "Bus or minibus or coach": "Bus, minibus, coach",
            "Motorbike or scooter or moped": "Motorbike, scooter or moped",
            "Taxi or minicab": "Taxi/minicab",
            "Underground or Metro or Light Rail or Tram": "Underground, metro, light rail, tram",
        },
        "ability_to_socially_distance_at_work_or_education": {
            "Difficult to maintain 2 metres apart. But you can usually be at least 1 metre away from other people": "Difficult to maintain 2m, but can be 1m",  # noqa: E501
            "Easy to maintain 2 metres apart. It is not a problem to stay this far away from other people": "Easy to maintain 2m",  # noqa: E501
            "Relatively easy to maintain 2 metres apart. Most of the time you can be 2 meters away from other people": "Relatively easy to maintain 2m",  # noqa: E501
            "Very difficult to be more than 1m away as your work means you are in close contact with others on a regular basis": "Very difficult to be more than 1m away",  # noqa: E501
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
            "I cover my face for other reasons - for example for religious or cultural reasons": "My face is already covered",  # noqa: E501
        },
        "face_covering_other_enclosed_places": {
            "Prefer not to say": None,
            "Yes sometimes": "Yes, sometimes",
            "Yes always": "Yes, always",
            "I am not going to other enclosed public spaces or using public transport": "Not going to other enclosed public spaces or using public transport",  # noqa: E501
            "I cover my face for other reasons - for example for religious or cultural reasons": "My face is already covered",  # noqa: E501
        },
        "other_covid_infection_test_results": {
            "All tests failed": "All Tests failed",
            "One or more tests were negative and none were positive": "Any tests negative, but none positive",
            "One or more tests were positive": "One or more positive test(s)",
        },
        "other_antibody_test_results": {
            "All tests failed": "All Tests failed",
            "One or more tests were negative for antibodies and none were positive": "Any tests negative, but none positive",  # noqa: E501
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
    df = update_column_values_from_map(
        df,
        "survey_completion_status",
        {
            "In Progress": "Partially Completed",
            "IN PROGRESS": "Partially Completed",
            "Submitted": "Completed",
            "SUBMITTED": "Completed",
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
    return df


def transform_survey_responses_generic(df: DataFrame) -> DataFrame:
    """
    Generic transformation steps to be applied to all survey response records.
    """
    raw_copy_list = [
        "think_had_covid_any_symptoms",
        "think_have_covid_symptom_any",
        "work_main_job_title",
        "work_main_job_role",
        "work_health_care_patient_facing",
        "work_health_care_area",
        "work_status_v1",
        "work_status_v2",
        "work_social_care",
        "work_not_from_home_days_per_week",
        "work_location",
        "sex",
        "participant_withdrawal_reason",
        "blood_sample_barcode",
        "swab_sample_barcode",
    ]
    df = assign_raw_copies(df, [column for column in raw_copy_list if column in df.columns])
    df = assign_unique_id_column(
        df, "unique_participant_response_id", concat_columns=["visit_id", "participant_id", "visit_datetime"]
    )
    df = assign_date_from_filename(df, "file_date", "survey_response_source_file")
    df = assign_column_regex_match(
        df,
        "bad_email",
        reference_column="email_address",
        pattern=r"/^w+[+.w-]*@([w-]+.)*w+[w-]*.([a-z]{2,4}|d+)$/i",
    )
    df = clean_postcode(df, "postcode")
    df = assign_outward_postcode(df, "outward_postcode", reference_column="postcode")

    consent_cols = ["consent_16_visits", "consent_5_visits", "consent_1_visit"]
    if all(col in df.columns for col in consent_cols):
        df = assign_consent_code(df, "consent_summary", reference_columns=consent_cols)

    df = assign_date_difference(df, "days_since_think_had_covid", "think_had_covid_onset_date", "visit_datetime")
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
            "work_sector",
            "work_health_care_area",
            "work_status_v2",
            "work_location",
            "work_direct_contact_patients_or_clients",
            "work_nursing_or_residential_care_home",
            "survey_response_type",
            "household_visit_status",
            "participant_survey_status",
            "self_isolating_reason",
            "ability_to_socially_distance_at_work_or_education",
            "transport_to_work_or_education",
            "face_covering_outside_of_home",
            "face_covering_work_or_education",
            "face_covering_other_enclosed_places",
            "other_antibody_test_location",
            "participant_withdrawal_reason",
            "cis_covid_vaccine_type",
            "cis_covid_vaccine_number_of_doses",
            "work_not_from_home_days_per_week",
            "times_shopping_last_7_days",
            "times_socialising_last_7_days",
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
        "times_socialising_last_7_days": times_value_map,
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
        "face_covering_work_or_education": {
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


def transform_survey_responses_version_2_delta(df: DataFrame) -> DataFrame:
    """
    Transformations that are specific to version 2 survey responses.
    """
    df = assign_taken_column(df=df, column_name_to_assign="swab_taken", reference_column="swab_sample_barcode")
    df = assign_taken_column(df=df, column_name_to_assign="blood_taken", reference_column="blood_sample_barcode")

    df = assign_column_uniform_value(df, "survey_response_dataset_major_version", 2)

    # Edits to final V2 values, which differ from raw
    df = update_column_values_from_map(
        df=df,
        column="self_isolating_reason",
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
    df = derive_work_status_columns(df)
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
    # TODO - not needed until later release
    # df = update_think_have_covid_symptom_any(
    #     df=df,
    #     column_name_to_update="think_have_covid_symptom_any",
    #     count_reference_column="think_have_covid_symptom_count",
    # )

    # df = assign_true_if_any(
    #     df=df,
    #     column_name_to_assign="any_think_have_covid_symptom_or_now",
    #     reference_columns=["think_have_covid_symptom_any", "think_have_covid"],
    #     true_false_values=["Yes", "No"],
    # )

    # df = assign_any_symptoms_around_visit(
    #     df=df,
    #     column_name_to_assign="any_symptoms_around_visit",
    #     symptoms_bool_column="any_think_have_covid_symptom_or_now",
    #     id_column="participant_id",
    #     visit_date_column="visit_datetime",
    #     visit_id_column="visit_id",
    # )

    # df = assign_true_if_any(
    #     df=df,
    #     column_name_to_assign="think_have_covid_cghfevamn_symptom_group",
    #     reference_columns=[
    #         "think_have_covid_symptom_cough",
    #         "think_have_covid_symptom_fever",
    #         "think_have_covid_symptom_loss_of_smell",
    #         "think_have_covid_symptom_loss_of_taste",
    #     ],
    #     true_false_values=["Yes", "No"],
    # )
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
    # df = assign_any_symptoms_around_visit(
    #     df=df,
    #     column_name_to_assign="symptoms_around_cghfevamn_symptom_group",
    #     id_column="participant_id",
    #     symptoms_bool_column="think_have_covid_cghfevamn_symptom_group",
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
        covered_work_column="face_covering_work_or_education",
    )

    return df


def union_dependent_derivations(df):
    """
    Transformations that must be carried out after the union of the different survey response schemas.
    """
    df = assign_fake_id(df, "ordered_household_id", "ons_household_id")
    df = assign_visit_order(df, "visit_order", "visit_datetime", "participant_id")
    df = symptom_column_transformations(df)
    df = create_formatted_datetime_string_columns(df)
    df = derive_age_columns(df, "age_at_visit")
    if "survey_completion_status" in df.columns:
        df = df.withColumn(
            "participant_visit_status", F.coalesce(F.col("participant_visit_status"), F.col("survey_completion_status"))
        )
    ethnicity_map = {
        "White": ["White-British", "White-Irish", "White-Gypsy or Irish Traveler", "Any other white background"],
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
    if "swab_sample_barcode_user_entered" in df.columns:
        for test_type in ["swab", "blood"]:
            df = df.withColumn(
                f"{test_type}_sample_barcode_combined",
                F.when(
                    F.col(f"{test_type}_sample_barcode_correct") == "No",
                    F.col(f"{test_type}_sample_barcode_user_entered"),
                ).otherwise(F.col(f"{test_type}_sample_barcode"))
                # set to sample_barcode if _sample_barcode_correct is yes or null.
            )
    df = assign_column_from_mapped_list_key(
        df=df, column_name_to_assign="ethnicity_group", reference_column="ethnicity", map=ethnicity_map
    )
    df = assign_ethnicity_white(
        df, column_name_to_assign="ethnicity_white", ethnicity_group_column_name="ethnicity_group"
    )
    # df = assign_work_patient_facing_now(
    #     df, "work_patient_facing_now", age_column="age_at_visit", work_healthcare_column="work_health_care_patient_facing"
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
    #     symptoms_bool_column="think_have_covid_cghfevamn_symptom_group",
    #     id_column="participant_id",
    #     visit_date_column="visit_datetime",
    #     visit_id_column="visit_id",
    # )
    df = derive_people_in_household_count(df)
    df = update_column_values_from_map(
        df=df,
        column="smokes_nothing_now",
        map={"Yes": "No", "No": "Yes"},
        condition_column="currently_smokes_or_vapes",
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


def fill_forwards_transformations(df):
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
            "work_sector",
            "work_sector_other",
            "work_social_care",
            "work_health_care_patient_facing",
            "work_health_care_area",
            "work_nursing_or_residential_care_home",
            "work_direct_contact_patients_or_clients",
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


def fill_forwards_travel_column(df):
    df = update_to_value_if_any_not_null(
        df=df,
        column_name_to_assign="been_outside_uk",
        value_to_assign="Yes",
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
        visit_date_column="visit_datetime",
        record_changed_column="been_outside_uk",
        record_changed_value="Yes",
    )
    return df


def impute_key_columns(df: DataFrame, imputed_value_lookup_df: DataFrame, columns_to_fill: list, log_directory: str):
    """
    Impute missing values for key variables that are required for weight calibration.
    Most imputations require geographic data being joined onto the participant records.
    Returns a single record per participant.
    """
    unique_id_column = "participant_id"
    for column in columns_to_fill:
        df = impute_and_flag(
            df,
            imputation_function=impute_by_ordered_fill_forward,
            reference_column=column,
            column_identity=unique_id_column,
            order_by_column="visit_datetime",
            order_type="asc",
        )
        df = impute_and_flag(
            df,
            imputation_function=impute_by_ordered_fill_forward,
            reference_column=column,
            column_identity=unique_id_column,
            order_by_column="visit_datetime",
            order_type="desc",
        )
    deduplicated_df = df.dropDuplicates([unique_id_column] + columns_to_fill)

    if imputed_value_lookup_df is not None:
        deduplicated_df = merge_previous_imputed_values(deduplicated_df, imputed_value_lookup_df, unique_id_column)

    deduplicated_df = impute_and_flag(
        deduplicated_df,
        imputation_function=impute_by_mode,
        reference_column="ethnicity_white",
        group_by_column="ons_household_id",
    )

    deduplicated_df = impute_and_flag(
        deduplicated_df,
        impute_by_k_nearest_neighbours,
        reference_column="ethnicity_white",
        donor_group_columns=["cis_area_code_20"],
        donor_group_column_weights=[5000],
        log_file_path=log_directory,
    )

    deduplicated_df = impute_and_flag(
        deduplicated_df,
        imputation_function=impute_by_distribution,
        reference_column="sex",
        group_by_columns=["ethnicity_white", "region_code"],
        first_imputation_value="Female",
        second_imputation_value="Male",
    )

    deduplicated_df = impute_and_flag(
        deduplicated_df,
        impute_by_k_nearest_neighbours,
        reference_column="date_of_birth",
        donor_group_columns=["region_code", "people_in_household_count_group", "work_status_group"],
        log_file_path=log_directory,
    )

    return deduplicated_df.select(
        unique_id_column,
        *columns_to_fill,
        *[col for col in deduplicated_df.columns if col.endswith("_imputation_method")],
        *[col for col in deduplicated_df.columns if col.endswith("_is_imputed")],
    )


def nims_transformations(df: DataFrame) -> DataFrame:
    """Clean and transform NIMS data after reading from table."""
    df = rename_column_names(df, column_name_maps["nims_column_name_map"])
    df = assign_column_to_date_string(df, "nims_vaccine_dose_1_date", reference_column="nims_vaccine_dose_1_datetime")
    df = assign_column_to_date_string(df, "nims_vaccine_dose_2_date", reference_column="nims_vaccine_dose_2_datetime")

    # TODO: Derive nims_linkage_status, nims_vaccine_classification, nims_vaccine_dose_1_time, nims_vaccine_dose_2_time
    return df


def derive_overall_vaccination(df: DataFrame) -> DataFrame:
    """Derive overall vaccination status from NIMS and CIS data."""
    return df
