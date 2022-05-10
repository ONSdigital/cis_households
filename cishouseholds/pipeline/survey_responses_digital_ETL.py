from cishouseholds.derive import derive_had_symptom_last_7days_from_digital


def digital_specific_cleaning(df):
    df = derive_had_symptom_last_7days_from_digital(df, "had_symptom_last_7days", "think_have_covid_symptom_.{1,}")
    return df
