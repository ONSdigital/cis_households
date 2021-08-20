from cishouseholds.derive import assign_column_regex_match, assign_column_uniform_value, assign_column_convert_to_date, assign_single_column_from_split, assign_consent_code

def survey_responses_version_2_ETL():
    extract_survey_responses_version_2_delta()
    transform_survey_responses_version_2_delta()
    load_survey_responses_version_2_delta()


def extract_survey_responses_version_2_delta():
    pass


def transform_survey_responses_version_2_delta():
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

    df = 1 # delete -- placeholder
    df = assign_column_uniform_value(df, "dataset", n=1) #replace 'n' with chosen value
    df = assign_column_regex_match(df, "bad_email", "email", "/^w+[+.w-]*@([w-]+.)*w+[w-]*.([a-z]{2,4}|d+)$/i") #using default email pattern regex to filter 'good' and 'bad' emails
    df = assign_column_convert_to_date(df, "visit_date", "visit_date_time")
    df = assign_column_convert_to_date(df, "sample_taken_date",	"sample_taken_date_time")
    # df = placeholder_for_derivation_number_7-2(df, "week")#derviation number 7 has been used twice - currently associated to ctpatterns
    # df = placeholder_for_derivation_number_7-2(df, "month")
    df = assign_single_column_from_split(df, "outer_postcode", "pcds", " ", 0)# splits on space between postcode segments and gets left half
    # df = placeholder_for_derivation_number_17(df, "country_barcode", ["swab_barcode_cleaned","blood_barcode_cleaned"], {0:"ONS", 1:"ONW", 2:"ONN", 3:"ONC"})
    # df = placeholder_for_derivation_number_18(df, "hh_id_fake",	"ons_household_id")
    df = assign_consent_code(df, "consent",	["consent_v16", "consent_v5", "consent_v1"])
    # df = placeholder_for_derivation_number_20(df, "work_healthcare", ["work_healthcare_v1", "work_direct_contact"])
    # df = placeholder_for_derivation_number_21(df, "work_socialcare", ["work_sector", "work_care_nursing_home" , "work_direct_contact"])
    # df = placeholder_for_derivation_number_10(df, "self_isolating", "self_isolating_v1")
    # df = placeholder_for_derivation_number_10(df, "contact_hospital", ["contact_participant_hospital", "contact_other_in_hh_hospital"])
    # df = placeholder_for_derivation_number_10(df, "contact_carehome", ["contact_participant_carehome", "contact_other_in_hh_carehome"])
    # df = placeholder_for_derivation_number_22(df, "age_at_visit", "visit_date,dob")
    # df = placeholder_for_derivation_number_23(df, "work_status", ["work_status_v1", "work_status_v2"])

    return df


def load_survey_responses_version_2_delta():
    pass
