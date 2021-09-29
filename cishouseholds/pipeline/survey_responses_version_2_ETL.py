from pyspark.accumulators import AddingAccumulatorParam
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

from cishouseholds.derive import assign_column_convert_to_date
from cishouseholds.derive import assign_column_regex_match
from cishouseholds.derive import assign_column_uniform_value
from cishouseholds.derive import assign_consent_code
from cishouseholds.derive import assign_single_column_from_split
from cishouseholds.extract import read_csv_to_pyspark_df
from cishouseholds.pipeline.declare_ETL import add_ETL
from cishouseholds.pipeline.input_variable_names import iqvia_v2_variable_name_map
from cishouseholds.pipeline.validation_schema import iqvia_v2_validation_schema
from cishouseholds.pyspark_utils import convert_cerberus_schema_to_pyspark
from cishouseholds.pyspark_utils import get_or_create_spark_session
from cishouseholds.validate import validate_and_filter


@add_ETL("survey_responses_version_2_ETL")
def survey_responses_version_2_ETL(delta_file_path: str):
    """
    End to end processing of a IQVIA survey responses CSV file.
    """

    spark_session = get_or_create_spark_session()
    iqvia_v2_spark_schema = convert_cerberus_schema_to_pyspark(iqvia_v2_validation_schema)
    iqvia_v2_spark_schema = None

    raw_iqvia_v2_data_header = ",".join(iqvia_v2_variable_name_map.keys())
    df = read_csv_to_pyspark_df(
        spark_session, delta_file_path, raw_iqvia_v2_data_header, iqvia_v2_spark_schema, sep="|"
    )

    error_accumulator = spark_session.sparkContext.accumulator(
        value=[], accum_param=AddingAccumulatorParam(zero_value=[])
    )

    df = validate_and_filter(df, iqvia_v2_validation_schema, error_accumulator)
    # df = transform_survey_responses_version_2_delta(spark_session, df)
    df = load_survey_responses_version_2_delta(spark_session, df)
    return df


def extract_survey_responses_version_2_delta(spark_session: SparkSession, df: DataFrame) -> DataFrame:
    pass


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
    # df = placeholder_for_derivation_number_22(df, "age_at_visit", "visit_date,dob")
    # df = placeholder_for_derivation_number_23(df, "work_status", ["work_status_v1", "work_status_v2"])

    return df


def load_survey_responses_version_2_delta(spark_session: SparkSession, df: DataFrame) -> DataFrame:
    return df
