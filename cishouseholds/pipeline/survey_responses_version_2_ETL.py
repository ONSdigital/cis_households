from itertools import chain

from pyspark.accumulators import AddingAccumulatorParam
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

from cishouseholds.derive import assign_age_at_date
from cishouseholds.derive import assign_column_convert_to_date
from cishouseholds.derive import assign_column_regex_match
from cishouseholds.derive import assign_column_uniform_value
from cishouseholds.derive import assign_consent_code
from cishouseholds.derive import assign_named_buckets
from cishouseholds.derive import assign_school_year_september_start
from cishouseholds.derive import assign_single_column_from_split
from cishouseholds.derive import assign_work_patient_facing_now
from cishouseholds.derive import assign_work_person_facing_now
from cishouseholds.edit import convert_columns_to_timestamps
from cishouseholds.edit import convert_null_if_not_in_list
from cishouseholds.edit import format_string_upper_and_clean
from cishouseholds.edit import update_schema_types
from cishouseholds.edit import update_work_facing_now_column
from cishouseholds.extract import read_csv_to_pyspark_df
from cishouseholds.pipeline.input_variable_names import iqvia_v2_variable_name_map
from cishouseholds.pipeline.pipeline_stages import register_pipeline_stage
from cishouseholds.pipeline.timestamp_map import iqvia_v2_time_map
from cishouseholds.pipeline.validation_schema import iqvia_v2_validation_schema
from cishouseholds.pyspark_utils import convert_cerberus_schema_to_pyspark
from cishouseholds.pyspark_utils import get_or_create_spark_session
from cishouseholds.validate import validate_and_filter


@register_pipeline_stage("survey_responses_version_2_ETL")
def survey_responses_version_2_ETL(delta_file_path: str):
    """
    End to end processing of a IQVIA survey responses CSV file.
    """
    spark_session = get_or_create_spark_session()
    iqvia_v2_spark_schema = convert_cerberus_schema_to_pyspark(iqvia_v2_validation_schema)

    raw_iqvia_v2_data_header = "|".join(iqvia_v2_variable_name_map.keys())
    df = read_csv_to_pyspark_df(
        spark_session, delta_file_path, raw_iqvia_v2_data_header, iqvia_v2_spark_schema, sep="|"
    )

    df = convert_columns_to_timestamps(df, iqvia_v2_time_map)
    iqvia_v2_time_map_list = list(chain(*list(iqvia_v2_time_map.values())))
    _iqvia_v2_validation_schema = update_schema_types(
        iqvia_v2_validation_schema, iqvia_v2_time_map_list, {"type": "timestamp"}
    )

    error_accumulator = spark_session.sparkContext.accumulator(
        value=[], accum_param=AddingAccumulatorParam(zero_value=[])
    )
    df = validate_and_filter(df, _iqvia_v2_validation_schema, error_accumulator)
    # df = transform_survey_responses_version_2_delta(spark_session, df)
    df = load_survey_responses_version_2_delta(spark_session, df)
    return df


def transform_survey_responses_version_2_delta(spark_session: SparkSession, df: DataFrame) -> DataFrame:
    """
    Call functions to process input for iqvia version 2 survey deltas.
    D11: assign_column_uniform_value
    D12: assign_column_regex_match
    D13: assign_column_convert_to_date
    D14: assign_single_column_from_split
    D19: assign_consent_code
    D15: school_year
    D17: country_barcode
    D18: hh_id_fake
    D19: consent
    D20: work_healthcare
    D21: work_socialcare
    D10: self_isolating
    D10: contact_hospital
    D10: contact_carehome
    D22: age_at_visit
    D23: work_status

    Parameters
    ----------
    df: pyspark.sql.DataFrame

    Return
    ------
    df: pyspark.sql.DataFrame
    """
    df = assign_column_uniform_value(df, "dataset", 1)  # replace 'n' with chosen value
    df = assign_column_regex_match(
        df, "bad_email", "email", r"/^w+[+.w-]*@([w-]+.)*w+[w-]*.([a-z]{2,4}|d+)$/i"
    )  # using default email pattern regex to filter 'good' and 'bad' emails
    df = assign_column_convert_to_date(df, "visit_date", "visit_datetime")
    df = assign_column_convert_to_date(df, "sample_taken_date", "samples_taken_datetime")
    df = assign_column_convert_to_date(df, "date_of_birth", "date_of_birth")
    df = format_string_upper_and_clean(df, "work_main_job_title")
    df = format_string_upper_and_clean(df, "work_main_job_role")
    df = convert_null_if_not_in_list(df, "sex", ["Male", "Female"])
    # df = placeholder_for_derivation_number_7-2(df, "week")
    # derviation number 7 has been used twice - currently associated to ctpatterns
    # df = placeholder_for_derivation_number_7-2(df, "month")
    df = assign_single_column_from_split(
        df, "outer_postcode", "postcode", " ", 0
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
    df = assign_age_at_date(df, "age_at_visit", "visit_date", "date_of_birth")
    df = assign_named_buckets(
        df, "age_at_visit", "ageg_small", {2: "2-11", 12: "12-19", 20: "20-49", 50: "50-69", 70: "70+"}
    )
    df = assign_named_buckets(df, "age_at_visit", "age_group_large_range", {16: "16-49", 50: "50-70", 70: "70+"})
    df = assign_named_buckets(
        df,
        "age_at_visit",
        "ageg_7",
        {2: "2-11", 12: "12-16", 17: "17-25", 25: "25-34", 35: "35-49", 50: "50-69", 70: "70+"},
    )
    df = assign_named_buckets(
        df,
        "age_at_visit",
        "age_group_large_range",
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
    df = assign_work_patient_facing_now(df, "work_patient_facing_now", "age_at_visit", "work_healthcare")
    df = assign_work_person_facing_now(df, "work_person_facing_now", "work_person_facing_now", "work_social_care")
    df = update_work_facing_now_column(
        df, "work_patient_facing_now", "work_status", ["Furloughed", "not working", "student"]
    )
    df = update_work_facing_now_column(
        df, "work_person_facing_now", "work_status", ["Furloughed", "not working", "student"]
    )
    # df = placeholder_for_derivation_number_23(df, "work_status", ["work_status_v1", "work_status_v2"])
    return df


def load_survey_responses_version_2_delta(spark_session: SparkSession, df: DataFrame) -> DataFrame:
    return df
