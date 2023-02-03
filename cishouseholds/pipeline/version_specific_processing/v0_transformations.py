# flake8: noqa
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from cishouseholds.derive import assign_column_uniform_value
from cishouseholds.derive import assign_taken_column
from cishouseholds.edit import apply_value_map_multiple_columns
from cishouseholds.edit import clean_barcode
from cishouseholds.edit import conditionally_set_column_values


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
