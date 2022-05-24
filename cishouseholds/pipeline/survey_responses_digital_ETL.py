import pyspark.sql.functions as F

from cishouseholds.derive import assign_raw_copies
from cishouseholds.derive import concat_fields_if_true
from cishouseholds.derive import derive_had_symptom_last_7days_from_digital
from cishouseholds.edit import apply_value_map_multiple_columns


def digital_specific_cleaning(df):
    df = assign_raw_copies(
        df,
        [
            "work_in_additional_paid_employment",
            "self_isolating",
            "illness_lasting_over_12_months",
            "ever_smoked_regularly",
            "currently_currently_smokes_or_vapes",
            "hours_a_day_with_someone_else_at_home",
            "face_covering_work_or_education_or_education",
        ],
    )
    df = apply_value_map_multiple_columns(
        df,
        {
            "work_in_additional_paid_employment": {"prefer_not_to_say": None},
            "self_isolating": {"prefer_not_to_say": None},
            "illness_lasting_over_12_months": {"prefer_not_to_say": None},
            "ever_smoked_regularly": {"prefer_not_to_say": None},
            "currently_currently_smokes_or_vapes": {"prefer_not_to_say": None},
            "hours_a_day_with_someone_else_at_home": {"prefer_not_to_say": None},
            "face_covering_work_or_education_or_education": {"prefer_not_to_say": None},
        },
    )


def digital_specific_transformations(df):
    df = df.withColumn("face_covering_outside_of_home", F.lit(None).cast("string"))
    df = concat_fields_if_true(df, "think_had_covid_which_symptoms", "think_had_covid_which_symptom_", "Yes", ";")
    df = concat_fields_if_true(df, "which_symptoms_last_7_days", "think_have_covid_symptom_", "Yes", ";")
    df = concat_fields_if_true(
        df, "think_have_long_covid_symptom_symptoms", "think_have_long_covid_symptom_symptom_", "Yes", ";"
    )

    df = derive_had_symptom_last_7days_from_digital(
        df,
        "had_symptom_last_7days",
        "think_have_covid_symptom_",
        [
            "fever",
            "muscle_ache_myalgia",
            "fatigue_weakness",
            "sore_throat",
            "cough",
            "shortness_of_breath",
            "headache",
            "nausea_vomiting",
            "abdominal_pain",
            "diarrhoea",
            "loss_of_taste",
            "loff_of_smell",
        ],
    )
    return df
