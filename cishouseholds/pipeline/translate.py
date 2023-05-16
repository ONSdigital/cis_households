import os
import shutil
from datetime import datetime
from pathlib import Path

import pyspark.sql.functions as F
from pandas import pandas as pd
from pyspark.sql.dataframe import DataFrame

from cishouseholds.edit import update_from_lookup_df
from cishouseholds.hdfs_utils import rename
from cishouseholds.pipeline.config import get_config
from cishouseholds.pipeline.generate_outputs import write_csv_rename
from cishouseholds.pipeline.input_file_processing import extract_lookup_csv
from cishouseholds.pipeline.load import add_error_file_log_entry
from cishouseholds.pipeline.validation_schema import validation_schemas  # noqa: F401
from cishouseholds.pyspark_utils import get_or_create_spark_session


def translate_welsh_survey_responses(df: DataFrame) -> DataFrame:
    """
    High-level function that manages the translation of free-text Welsh responses in the
    survey response extract.

    Loads values from config to determine if the translation_project_workflow_enabled flag is true, if the
    translation_project_workflow_enabled flag is true then it works through the translation project workflow to:
    1. check for new completed free-text translations
    2. if there are new completed free-text translations then backup and update the translations_lookup_df
    3. update the df from the translations_lookup_df
    4. filter the df to get free-text responses to be translated
    5. export free-text responses to be translated

    If the translation_project_workflow_enabled flag is false then it will just carry out step 3. above.

    This function requires that the config contains a valid translation_lookup_path in the storage dictionary of
    the pipeline_config file.

    Parameters
    ----------
    df :  DataFrame
        df containing free-text responses to translate)

    Returns
    -------
    df : DataFrame
        df incorporating any available translations to free-text responses
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

    return df


def get_new_translations_from_completed_translations_directory(
    translation_directory: str,
    translation_lookup_path: str,
) -> DataFrame:
    """
    Checks for new completed translations in a subfolder of the translation_directory, builds a df of
    new_translations from all xlsx files found in this subfolder.
    Loads the existing translation_lookup_df from the translation_lookup_path and compares it against
    the new_translations df, and filters the new_translations to only rows which have not previously
    been translated.

    Parameters
    ----------
    translation_directory : str
        directory for translations workflow
    translation_lookup_path : str
        path of existing translated values lookup

    Returns
    -------
    new_translations_df : DataFrame
        returns a new_translations_df of only new translation rows.
    """

    completed_translations_directory = os.path.join(translation_directory, "completed/")

    list_of_file_paths = [
        os.path.join(completed_translations_directory, _)
        for _ in os.listdir(completed_translations_directory)
        if _.endswith(".xlsx")
    ]

    new_translations = pd.DataFrame(
        columns=["id_column_name", "id", "dataset_name", "target_column_name", "old_value", "new_value"]
    )
    for path in list_of_file_paths:
        translated_workbook = pd.ExcelFile(path, engine="openpyxl")
        translated_sheets = translated_workbook.sheet_names
        workbook_translations = pd.DataFrame()
        for sheet in translated_sheets:
            try:
                sheet_translation = pd.read_excel(
                    path, sheet_name=sheet, engine="openpyxl", usecols=["target_column_name", "original", "translated"]
                )
            except ValueError:
                message = "Sheet could not be read correctly. Check input sheet"
                add_error_file_log_entry(path, message)  # type: ignore
                continue
            sheet_translation = (
                sheet_translation.dropna()
                .assign(id_column_name="participant_completion_window_id", id=sheet, dataset_name=None)
                .reindex(
                    columns=["id_column_name", "id", "dataset_name", "target_column_name", "original", "translated"]
                )
                .rename(columns={"original": "old_value", "translated": "new_value"})
            )
            if len(sheet_translation) > 0:
                sheet_translation.loc[sheet_translation.index.max() + 1] = [
                    "participant_completion_window_id",
                    sheet,
                    None,
                    "form_language_launch",
                    "Welsh",
                    "Translated",
                ]
            workbook_translations = workbook_translations.append(sheet_translation)
        new_translations = new_translations.append(workbook_translations)
        # Redo the file move to take them to a new directory in HUE rather than CDSW
        if os.path.exists(os.path.join(completed_translations_directory, "processed/")):
            shutil.move(path, os.path.join(completed_translations_directory, "processed/"))

    translation_lookup_df = extract_lookup_csv(
        translation_lookup_path, validation_schemas["csv_lookup_schema_extended"]
    ).toPandas()

    filtered_new_translations = new_translations[~new_translations["id"].isin(translation_lookup_df["id"])]
    spark_session = get_or_create_spark_session()
    new_translations_df = spark_session.createDataFrame(
        filtered_new_translations,
        schema="id_column_name string, id string, dataset_name string, target_column_name string, old_value string, new_value string",
    )

    return new_translations_df


def backup_and_replace_translation_lookup_df(
    new_lookup_df: DataFrame,
    lookup_directory: str,
    backup_directory: str,
    formatted_time: str = datetime.now().strftime("%Y%m%d_%H%M"),
):
    """
    Backs up the lookup_path to the backup_directory with an automatically generated timestamp suffix.
    Writes the new_lookup_df to the lookup_directory

    This uses two rename operations instead of a delete / write_csv_rename operations as there were obstinate
    technical difficulties getting these to work as intended on HDFS

    Parameters
    ----------
    new_lookup_df : DataFrame
        new_lookup_df to replace old_lookup_df
    lookup_directory : str
        directory containing the lookup_df
    backup_directory : str
        directory to back up the old_lookup_df to
    formatted_time : str, optional
        defaults to datetime.now().strftime("%Y%m%d_%H%M").
    """
    backup_path = Path(backup_directory) / f"translated_value_lookup_{formatted_time}.csv"
    lookup_path = Path(lookup_directory) / "translated_value_lookup.csv"
    temp_path = Path(lookup_directory) / f"translated_value_lookup_{formatted_time}"
    write_csv_rename(new_lookup_df, temp_path, ",", ".csv")
    rename(str(lookup_path), str(backup_path))
    temp_path = Path(lookup_directory) / f"translated_value_lookup_{formatted_time}.csv"
    rename(str(temp_path), str(lookup_path))


def export_responses_to_be_translated_to_translation_directory(
    to_be_translated_df: DataFrame,
    translation_directory: str,
    formatted_time: str = datetime.now().strftime("%Y%m%d_%H%M"),
):
    """
    Exports and formats all responses from the to_be_translated_df into an xlsx workbook containing
    one tab for each row to be translated in the df. Workbook is saved into the translation_directory with
    an automatically generated timestamp

    Parameters
    ----------
    to_be_translated_df : DataFrame
        to_be_translated_df containing all rows requiring free-text translation
    translation_directory : str
        directory for the translation workflow
    formatted_time : str, optional
        defaults to datetime.now().strftime("%Y%m%d_%H%M").
    """
    translations_workbook = translation_directory + f"/to_be_translated_{formatted_time}.xlsx"

    to_be_translated_df = to_be_translated_df.drop("form_language_launch").toPandas()
    unique_id_list = to_be_translated_df["participant_completion_window_id"].unique()

    with pd.ExcelWriter(translations_workbook, engine="openpyxl") as writer:
        for unique_id in unique_id_list:
            participant_to_be_translated_df = (
                to_be_translated_df.query(f'participant_completion_window_id == "{unique_id}"')
                .transpose()
                .assign(translated=([None] * len(to_be_translated_df.columns)))
            )
            participant_to_be_translated_df.index.name = "target_column_name"
            participant_to_be_translated_df.columns = ["original", "translated"]
            participant_to_be_translated_df.to_excel(writer, sheet_name=unique_id)
            writer.sheets[unique_id].column_dimensions["A"].width = 35
            writer.sheets[unique_id].column_dimensions["B"].width = 35
            writer.sheets[unique_id].column_dimensions["C"].width = 35


def translate_welsh_free_text_responses_digital(
    df: DataFrame,
    lookup_path: str,
) -> DataFrame:
    """
    Loads a lookup_df from the lookup_path and runs an update_from_lookup_df function to replace specific values with
    translated values as specified in the lookup_df

    Parameters
    ----------
    df : DataFrame
        df containing free-text responses requiring translation
    lookup_path : str
        path for translation_lookup_df containing responses to translate

    Returns
    -------
    df : DataFrame
        df containing translated free-text responses provided in the translation_lookup_df
    """

    translation_lookup_df = extract_lookup_csv(lookup_path, validation_schemas["csv_lookup_schema_extended"])
    df = update_from_lookup_df(df, translation_lookup_df, id_column="participant_completion_window_id")

    return df


def get_welsh_responses_to_be_translated(df: DataFrame) -> DataFrame:
    """
    Creates a unique id field for the purpose of translation matching using a field map, then filters
    the df using a free-text field map to return only records with at least one not null value

    Parameters
    ----------
    df : DataFrame
        df containing responses that may require translation

    Returns
    -------
    to_be_translated_df : DataFrame
        filtered df containing only responses that require free-text translation
    """
    free_text_columns = [
        "work_main_job_title",
        "work_main_job_role",
        "work_sector_other",
        "cis_covid_vaccine_type_other",
    ]
    to_be_translated_df = (
        df.select(free_text_columns + ["form_language_launch", "participant_completion_window_id"])
        .filter(F.col("form_language_launch") == "cy")
        .na.drop(how="all", subset=free_text_columns)
        .distinct()
    )
    return to_be_translated_df
