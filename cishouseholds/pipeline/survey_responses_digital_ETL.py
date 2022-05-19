from cishouseholds.derive import derive_had_symptom_last_7days_from_digital


def digital_specific_cleaning(df):
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
