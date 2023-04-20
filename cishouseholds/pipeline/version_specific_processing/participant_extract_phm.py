# flake8: noqa
from pyspark.sql.dataframe import DataFrame

from cishouseholds.derive import assign_column_from_mapped_list_key
from cishouseholds.edit import apply_value_map_multiple_columns
from cishouseholds.edit import convert_columns_to_timestamps
from cishouseholds.edit import rename_column_names
from cishouseholds.pipeline.mapping import column_name_maps
from cishouseholds.pipeline.timestamp_map import phm_datetime_map


def transform_participant_extract_phm(df: DataFrame) -> DataFrame:
    df = rename_column_names(df, column_name_maps["phm_participant_column_name_map"])
    df = convert_columns_to_timestamps(df, phm_datetime_map)
    df = preprocessing(df)
    return df


def preprocessing(df: DataFrame) -> DataFrame:
    """
    transform and process participant extract data received from cis digital
    """
    col_val_map = {
        "language_preference": {
            "CY": "Welsh",
            "EN": "English",
        },
        "participant_withdrawal_reason": {
            "Moving Location": "Moving location",
            "Bad experience with tester / survey": "Bad experience with interviewer/survey",
            "Swab / blood process too distressing": "Swab/blood process too distressing",
            "Do NOT Reinstate": "",
        },
        "ethnicity": {
            "African": "Black,Caribbean,African-African",
            "Caribbean": "Black,Caribbean,Afro-Caribbean",
            "Any other Black or African or Carribbean background": "Any other Black background",
            "Any other Mixed/Multiple background": "Any other Mixed background",
            "Bangladeshi": "Asian or Asian British-Bangladeshi",
            "Chinese": "Asian or Asian British-Chinese",
            "English, Welsh, Scottish, Northern Irish or British": "White-British",
            "Indian": "Asian or Asian British-Indian",
            "Irish": "White-Irish",
            "Pakistani": "Asian or Asian British-Pakistani",
            "White and Asian": "Mixed-White & Asian",
            "White and Black African": "Mixed-White & Black African",
            "White and Black Caribbean": "Mixed-White & Black Caribbean",
            "Gypsy or Irish Traveller": "White-Gypsy or Irish Traveller",
            "Arab": "Other ethnic group-Arab",
        },
    }
    ethnic_group_map = {
        "White": ["White-British", "White-Irish", "White-Gypsy or Irish Traveller", "Any other white background"],
        "Asian": [
            "Asian or Asian British-Indian",
            "Asian or Asian British-Pakistani",
            "Asian or Asian British-Bangladeshi",
            "Asian or Asian British-Chinese",
            "Any other Asian background",
        ],
        "Black": ["Black,Caribbean,African-African", "Black,Caribbean,Afro-Caribbean", "Any other Black background"],
        "Mixed": [
            "Mixed-White & Black Caribbean",
            "Mixed-White & Black African",
            "Mixed-White & Asian",
            "Any other Mixed background",
        ],
        "Other": ["Other ethnic group-Arab", "Any other ethnic group"],
    }

    df = assign_column_from_mapped_list_key(
        df=df, column_name_to_assign="ethnicity_group", reference_column="ethnic_group", map=ethnic_group_map
    )
    df = apply_value_map_multiple_columns(df, col_val_map)
    return df
