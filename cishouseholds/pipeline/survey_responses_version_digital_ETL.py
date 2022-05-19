from cishouseholds.derive import assign_column_uniform_value


def digital_specific_cleaning(df):
    df = assign_column_uniform_value(df, "survey_response_dataset_major_version", 3)
    return df
