import pyspark.sql.functions as F

from cishouseholds.derive import concat_fields_if_true
from cishouseholds.derive import derive_had_symptom_last_7days_from_digital


def digital_specific_transformations(df):
    df = df.withColumn("face_covering_outside_of_home", F.lit(None).cast("string"))
    df = concat_fields_if_true(df, "think_had_covid_which_symptoms", "think_had_covid_which_symptom_", "Yes", ";")
    df = concat_fields_if_true(df, "which_symptoms_last_7_days", "symptoms_last_7_days_", "Yes", ";")
    df = concat_fields_if_true(df, "long_covid_symptoms", "long_covid_symptom_", "Yes", ";")

    df = derive_had_symptom_last_7days_from_digital(
        df,
        "had_symptom_last_7days",
        "think_have_covid_symptom_",
        [
            "fever"
            "muscle_ache_myalgia"
            "fatigue_weakness"
            "sore_throat"
            "cough"
            "shortness_of_breath"
            "headache"
            "nausea_vomiting"
            "abdominal_pain"
            "diarrhoea"
            "loss_of_taste"
            "loff_of_smell"
        ],
    )
    return df
