from cishouseholds.derive import derive_self_isolating_from_digital


def digital_specific_cleaning(df):
    df = derive_self_isolating_from_digital(df, "are_you_self_isolating_s2", "self_isolating", "self_isolating_reason")
    return df
