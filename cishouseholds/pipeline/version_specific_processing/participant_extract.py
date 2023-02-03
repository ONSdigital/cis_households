# flake8: noqa
from pyspark.sql.dataframe import DataFrame

from cishouseholds.derive import assign_column_from_mapped_list_key
from cishouseholds.edit import apply_value_map_multiple_columns
from cishouseholds.pipeline.config import get_config
from cishouseholds.pipeline.high_level_transformations import create_formatted_datetime_string_columns
from cishouseholds.pipeline.input_file_processing import extract_lookup_csv
from cishouseholds.pipeline.translate import backup_and_replace_translation_lookup_df
from cishouseholds.pipeline.translate import export_responses_to_be_translated_to_translation_directory
from cishouseholds.pipeline.translate import get_new_translations_from_completed_translations_directory
from cishouseholds.pipeline.translate import get_welsh_responses_to_be_translated
from cishouseholds.pipeline.translate import translate_welsh_fixed_text_responses_digital
from cishouseholds.pipeline.translate import translate_welsh_free_text_responses_digital
from cishouseholds.pipeline.validation_schema import validation_schemas  # noqa: F401


def transform_participant_extract_digital(df: DataFrame) -> DataFrame:
    """
    transform and process participant extract data received from cis digital
    """
    col_val_map = {
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
    df = create_formatted_datetime_string_columns(df)

    return df


def translate_welsh_survey_responses_version_digital(df: DataFrame) -> DataFrame:
    """
    High-level function that manages the translation of both fixed-text and free-text Welsh responses in the
    CIS Digital response extract.

    Loads values from config to determine if the translation_project_workflow_enabled flag is true, if the
    translation_project_workflow_enabled flag is true then it works through the translation project workflow to:
    1. check for new completed free-text translations
    2. if there are new completed free-text translations then backup and update the translations_lookup_df
    3. update the df from the translations_lookup_df
    4. filter the df to get free-text responses to be translated
    5. export free-text responses to be translated

    If the translation_project_workflow_enabled flag is false then it will just carry out step 3. above.
    After this check it will update the fixed-text responses in the df.

    This function requires that the config contains a valid translation_lookup_path in the storage dictionary of
    the pipeline_config file.

    Parameters
    ----------
    df :  DataFrame
        df containing free- or fixed-text responses to translate)

    Returns
    -------
    df : DataFrame
        df incorporating any available translations to free- or fixed-text responses
    """
    translation_settings = get_config().get("translation", {"inactive": "inactive"})
    storage_settings = get_config().get("storage", {"inactive": "inactive"})
    translation_lookup_path = storage_settings.get("translation_lookup_path", "inactive")

    translation_settings_in_pipeline_config = translation_settings != {"inactive": "inactive"}
    translation_lookup_path_in_pipeline_config = translation_lookup_path != "inactive"

    if translation_settings_in_pipeline_config:

        translation_directory = translation_settings.get("translation_directory", None)
        translation_lookup_directory = translation_settings.get("translation_lookup_directory", None)
        translation_backup_directory = translation_settings.get("translation_backup_directory", None)

        new_translations_df = get_new_translations_from_completed_translations_directory(
            translation_directory=translation_directory,
            translation_lookup_path=translation_lookup_path,
        )

        if new_translations_df.count() > 0:
            translation_lookup_df = extract_lookup_csv(
                translation_lookup_path, validation_schemas["csv_lookup_schema_extended"]
            )

            new_translations_lookup_df = translation_lookup_df.union(new_translations_df)

            backup_and_replace_translation_lookup_df(
                new_lookup_df=new_translations_lookup_df,
                lookup_directory=translation_lookup_directory,
                backup_directory=translation_backup_directory,
            )

        df = translate_welsh_free_text_responses_digital(
            df=df,
            lookup_path=translation_lookup_path,
        )

        to_be_translated_df = get_welsh_responses_to_be_translated(df)

        if to_be_translated_df.count() > 0:
            export_responses_to_be_translated_to_translation_directory(
                to_be_translated_df=to_be_translated_df, translation_directory=translation_directory
            )

    if translation_lookup_path_in_pipeline_config and not translation_settings_in_pipeline_config:
        df = translate_welsh_free_text_responses_digital(
            df=df,
            lookup_path=translation_lookup_path,
        )

    df = translate_welsh_fixed_text_responses_digital(df)

    return df
