from cishouseholds.derive import derive_self_isolating_from_digital


def digital_specific_cleaning(df):
    df = derive_self_isolating_from_digital(df, "self_isolating_detailed", "self_isolating", "self_isolating_reason")
    return df
