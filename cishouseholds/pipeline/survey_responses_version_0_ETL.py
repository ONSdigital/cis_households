from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from cishouseholds.derive import assign_column_uniform_value
from cishouseholds.derive import assign_column_value_from_multiple_column_map
from cishouseholds.edit import apply_value_map_multiple_columns
from cishouseholds.edit import clean_barcode
from cishouseholds.edit import map_column_values_to_null


def transform_survey_responses_version_0_delta(df: DataFrame) -> DataFrame:
    """
    Call functions to process input for iqvia version 0 survey deltas.
    """
    df = assign_column_uniform_value(df, "survey_response_dataset_major_version", 0)
    df = df.withColumn("sex", F.coalesce(F.col("sex"), F.col("gender"))).drop("gender")

    df = map_column_values_to_null(
        df=df,
        value="Participant Would Not/Could Not Answer",
        column_list=[
            "ethnicity",
            "work_status_v0",
            "work_location",
            "visit_type",
            "withdrawal_reason",
            "work_not_from_home_days_per_week",
        ],
    )
    df = assign_column_value_from_multiple_column_map(
        df,
        "work_status_v0",
        [
            [1, [1, 1, None, None]],
            [4, [1, 2, None, None]],
            [1, [2, None, 1, None]],
            [2, [2, None, 2, None]],
            [2, [None, None, None]],
            [4, [2, None, None, None]],
            [4, [3, None, 1, None]],
            [4, [3, None, 2, None]],
            [4, [3, None, 3, None]],
            [4, [3, None, None, None]],
            [5, [4, None, None, 1]],
            [5, [4, None, None, 2]],
            [5, [4, None, None, 3]],
            [5, [4, None, None, 4]],
            [5, [4, None, None, 5]],
        ],
        ["work_status_digital", "work_status_employment", "work_status_unemployment", "work_status_education"],
    )
    column_editing_map = {
        "work_location": {
            "Both (working from home and working outside of your home)": "Both (from home and somewhere else)",
            "Working From Home": "Working from home",
            "Working Outside of your Home": "Working somewhere else (not your home)",
            "Not applicable": "Not applicable, not currently working",
        },
        "last_covid_contact_location": {
            "In your own household": "Living in your own home",
            "Outside your household": "Outside your home",
        },
        "last_suspected_covid_contact_location": {
            "In your own household": "Living in your own home",
            "Outside your household": "Outside your home",
        },
        "other_pcr_test_results": {
            "Positive": "One or more positive test(s)",
        },
    }
    df = apply_value_map_multiple_columns(df, column_editing_map)

    df = clean_barcode(df=df, barcode_column="swab_sample_barcode", edited_column="swab_sample_barcode_edited_flag")
    df = clean_barcode(df=df, barcode_column="blood_sample_barcode", edited_column="blood_sample_barcode_edited_flag")
    return df
