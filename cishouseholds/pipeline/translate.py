import os
import shutil
from datetime import datetime
from pathlib import Path

import pyspark.sql.functions as F
from pandas import pandas as pd
from pyspark.sql.dataframe import DataFrame

from cishouseholds.derive import translate_column_regex_replace
from cishouseholds.edit import apply_value_map_multiple_columns
from cishouseholds.edit import update_from_lookup_df
from cishouseholds.hdfs_utils import rename
from cishouseholds.pipeline.generate_outputs import write_csv_rename
from cishouseholds.pipeline.input_file_processing import extract_lookup_csv
from cishouseholds.pipeline.load import add_error_file_log_entry
from cishouseholds.pipeline.mapping import _welsh_ability_to_socially_distance_at_work_or_education_categories
from cishouseholds.pipeline.mapping import _welsh_blood_kit_missing_categories
from cishouseholds.pipeline.mapping import _welsh_blood_not_taken_reason_categories
from cishouseholds.pipeline.mapping import _welsh_blood_sample_not_taken_categories
from cishouseholds.pipeline.mapping import _welsh_cis_covid_vaccine_number_of_doses_categories
from cishouseholds.pipeline.mapping import _welsh_contact_type_by_age_group_categories
from cishouseholds.pipeline.mapping import _welsh_currently_smokes_or_vapes_description_categories
from cishouseholds.pipeline.mapping import _welsh_face_covering_categories
from cishouseholds.pipeline.mapping import _welsh_live_with_categories
from cishouseholds.pipeline.mapping import _welsh_lot_little_not_categories
from cishouseholds.pipeline.mapping import _welsh_number_of_types_categories
from cishouseholds.pipeline.mapping import _welsh_other_covid_infection_test_result_categories
from cishouseholds.pipeline.mapping import _welsh_self_isolating_reason_detailed_categories
from cishouseholds.pipeline.mapping import _welsh_swab_kit_missing_categories
from cishouseholds.pipeline.mapping import _welsh_swab_sample_not_taken_categories
from cishouseholds.pipeline.mapping import _welsh_transport_to_work_education_categories
from cishouseholds.pipeline.mapping import _welsh_vaccination_type_categories
from cishouseholds.pipeline.mapping import _welsh_work_location_categories
from cishouseholds.pipeline.mapping import _welsh_work_sector_categories
from cishouseholds.pipeline.mapping import _welsh_work_status_digital_categories
from cishouseholds.pipeline.mapping import _welsh_work_status_education_categories
from cishouseholds.pipeline.mapping import _welsh_work_status_employment_categories
from cishouseholds.pipeline.mapping import _welsh_work_status_unemployment_categories
from cishouseholds.pipeline.mapping import _welsh_yes_no_categories
from cishouseholds.pipeline.validation_schema import validation_schemas  # noqa: F401
from cishouseholds.pyspark_utils import get_or_create_spark_session


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
                .assign(id_column_name="id", id=sheet, dataset_name=None)
                .reindex(
                    columns=["id_column_name", "id", "dataset_name", "target_column_name", "original", "translated"]
                )
                .rename(columns={"original": "old_value", "translated": "new_value"})
            )
            if len(sheet_translation) > 0:
                sheet_translation.loc[sheet_translation.index.max() + 1] = [
                    "id",
                    sheet,
                    None,
                    "form_language",
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

    to_be_translated_df = to_be_translated_df.drop(
        "participant_id", "participant_completion_window_id", "form_language"
    ).toPandas()
    unique_id_list = to_be_translated_df["id"].unique()

    with pd.ExcelWriter(translations_workbook, engine="openpyxl") as writer:
        for unique_id in unique_id_list:
            participant_to_be_translated_df = (
                to_be_translated_df.query(f'id == "{unique_id}"')
                .transpose()
                .assign(translated=([None] * len(to_be_translated_df.columns)))
            )
            participant_to_be_translated_df.index.name = "target_column_name"
            participant_to_be_translated_df.columns = ["original", "translated"]
            participant_to_be_translated_df.to_excel(writer, sheet_name=unique_id)
            writer.sheets[unique_id].column_dimensions["A"].width = 35
            writer.sheets[unique_id].column_dimensions["B"].width = 35
            writer.sheets[unique_id].column_dimensions["C"].width = 35


def translate_welsh_fixed_text_responses_digital(df: DataFrame) -> DataFrame:
    """
    Uses column maps and dictionaries defined in mapping.py to translate other language fixed-text
    responses to English.

    Parameters
    ----------
    df : DataFrame
        the dataframe containing fixed text responses to translate

    Returns
    -------
    df : DataFrame
        the df containing translated fixed-text responses
    """
    digital_yes_no_columns = [
        "household_invited_to_digital",
        "household_members_under_2_years_count",
        "consent_nhs_data_share_yn",
        "consent_contact_extra_research_yn",
        "consent_use_of_surplus_blood_samples_yn",
        "consent_blood_samples_if_positive_yn",
        "participant_invited_to_digital",
        "participant_enrolled_digital",
        "opted_out_of_next_window",
        "opted_out_of_blood_next_window",
        "swab_taken",
        "questionnaire_started_no_incentive",
        "swab_returned",
        "blood_taken",
        "blood_returned",
        "work_in_additional_paid_employment",
        "work_nursing_or_residential_care_home",
        "work_direct_contact_patients_or_clients",
        "think_have_covid_symptom_fever",
        "think_have_covid_symptom_headache",
        "think_have_covid_symptom_muscle_ache",
        "think_have_covid_symptom_fatigue",
        "think_have_covid_symptom_nausea_or_vomiting",
        "think_have_covid_symptom_abdominal_pain",
        "think_have_covid_symptom_diarrhoea",
        "think_have_covid_symptom_sore_throat",
        "think_have_covid_symptom_cough",
        "think_have_covid_symptom_shortness_of_breath",
        "think_have_covid_symptom_loss_of_taste",
        "think_have_covid_symptom_loss_of_smell",
        "think_have_covid_symptom_more_trouble_sleeping",
        "think_have_covid_symptom_loss_of_appetite",
        "think_have_covid_symptom_runny_nose_or_sneezing",
        "think_have_covid_symptom_noisy_breathing",
        "think_have_covid_symptom_chest_pain",
        "think_have_covid_symptom_palpitations",
        "think_have_covid_symptom_vertigo_or_dizziness",
        "think_have_covid_symptom_anxiety",
        "think_have_covid_symptom_low_mood",
        "think_have_covid_symptom_memory_loss_or_confusion",
        "think_have_covid_symptom_difficulty_concentrating",
        "self_isolating",
        "think_have_covid",
        "illness_lasting_over_12_months",
        "ever_smoked_regularly",
        "currently_smokes_or_vapes",
        "cis_covid_vaccine_received",
        "cis_covid_vaccine_type_1",
        "cis_covid_vaccine_type_2",
        "cis_covid_vaccine_type_3",
        "cis_covid_vaccine_type_4",
        "cis_covid_vaccine_type_5",
        "cis_covid_vaccine_type_6",
        "cis_flu_vaccine_received",
        "been_outside_uk",
        "think_had_covid",
        "think_had_covid_any_symptoms",
        "think_had_covid_symptom_fever",
        "think_had_covid_symptom_headache",
        "think_had_covid_symptom_muscle_ache",
        "think_had_covid_symptom_fatigue",
        "think_had_covid_symptom_nausea_or_vomiting",
        "think_had_covid_symptom_abdominal_pain",
        "think_had_covid_symptom_diarrhoea",
        "think_had_covid_symptom_sore_throat",
        "think_had_covid_symptom_cough",
        "think_had_covid_symptom_shortness_of_breath",
        "think_had_covid_symptom_loss_of_taste",
        "think_had_covid_symptom_loss_of_smell",
        "think_had_covid_symptom_more_trouble_sleeping",
        "think_had_covid_symptom_loss_of_appetite",
        "think_had_covid_symptom_runny_nose_or_sneezing",
        "think_had_covid_symptom_noisy_breathing",
        "think_had_covid_symptom_chest_pain",
        "think_had_covid_symptom_palpitations",
        "think_had_covid_symptom_vertigo_or_dizziness",
        "think_had_covid_symptom_anxiety",
        "think_had_covid_symptom_low_mood",
        "think_had_covid_symptom_memory_loss_or_confusion",
        "think_had_covid_symptom_difficulty_concentrating",
        "think_had_covid_contacted_nhs",
        "think_had_covid_admitted_to_hospital",
        "other_covid_infection_test",
        "regularly_lateral_flow_testing",
        "other_antibody_test",
        "think_have_long_covid",
        "think_have_long_covid_symptom_fever",
        "think_have_long_covid_symptom_headache",
        "think_have_long_covid_symptom_muscle_ache",
        "think_have_long_covid_symptom_fatigue",
        "think_have_long_covid_symptom_nausea_or_vomiting",
        "think_have_long_covid_symptom_abdominal_pain",
        "think_have_long_covid_symptom_diarrhoea",
        "think_have_long_covid_symptom_loss_of_taste",
        "think_have_long_covid_symptom_loss_of_smell",
        "think_have_long_covid_symptom_sore_throat",
        "think_have_long_covid_symptom_cough",
        "think_have_long_covid_symptom_shortness_of_breath",
        "think_have_long_covid_symptom_loss_of_appetite",
        "think_have_long_covid_symptom_chest_pain",
        "think_have_long_covid_symptom_palpitations",
        "think_have_long_covid_symptom_vertigo_or_dizziness",
        "think_have_long_covid_symptom_anxiety",
        "think_have_long_covid_symptom_low_mood",
        "think_have_long_covid_symptom_more_trouble_sleeping",
        "think_have_long_covid_symptom_memory_loss_or_confusion",
        "think_have_long_covid_symptom_difficulty_concentrating",
        "think_have_long_covid_symptom_runny_nose_or_sneezing",
        "think_have_long_covid_symptom_noisy_breathing",
        "contact_known_positive_covid_last_28_days",
        "hospital_last_28_days",
        "other_household_member_hospital_last_28_days",
        "care_home_last_28_days",
        "other_household_member_care_home_last_28_days",
        "work_main_job_changed",
        "swab_sample_barcode_correct",
        "blood_sample_barcode_correct",
        "vaccinated_against_flu",
        "think_have_covid_symptoms",
        "contact_suspected_positive_covid_last_28_days",
    ]
    df = apply_value_map_multiple_columns(
        df,
        {k: _welsh_yes_no_categories for k in digital_yes_no_columns},
    )
    column_editing_map = {
        "physical_contact_under_18_years": _welsh_contact_type_by_age_group_categories,
        "physical_contact_18_to_69_years": _welsh_contact_type_by_age_group_categories,
        "physical_contact_over_70_years": _welsh_contact_type_by_age_group_categories,
        "social_distance_contact_under_18_years": _welsh_contact_type_by_age_group_categories,
        "social_distance_contact_18_to_69_years": _welsh_contact_type_by_age_group_categories,
        "social_distance_contact_over_70_years": _welsh_contact_type_by_age_group_categories,
        "times_hour_or_longer_another_home_last_7_days": _welsh_number_of_types_categories,
        "times_hour_or_longer_another_person_your_home_last_7_days": _welsh_number_of_types_categories,
        "times_shopping_last_7_days": _welsh_number_of_types_categories,
        "times_socialising_last_7_days": _welsh_number_of_types_categories,
        "cis_covid_vaccine_type": _welsh_vaccination_type_categories,
        "cis_covid_vaccine_number_of_doses": _welsh_cis_covid_vaccine_number_of_doses_categories,
        "cis_covid_vaccine_type_1": _welsh_vaccination_type_categories,
        "cis_covid_vaccine_type_2": _welsh_vaccination_type_categories,
        "cis_covid_vaccine_type_3": _welsh_vaccination_type_categories,
        "cis_covid_vaccine_type_4": _welsh_vaccination_type_categories,
        "cis_covid_vaccine_type_5": _welsh_vaccination_type_categories,
        "cis_covid_vaccine_type_6": _welsh_vaccination_type_categories,
        "illness_reduces_activity_or_ability": _welsh_lot_little_not_categories,
        "think_have_long_covid_symptom_reduced_ability": _welsh_lot_little_not_categories,
        "last_covid_contact_type": _welsh_live_with_categories,
        "last_suspected_covid_contact_type": _welsh_live_with_categories,
        "face_covering_work_or_education": _welsh_face_covering_categories,
        "face_covering_other_enclosed_places": _welsh_face_covering_categories,
        "swab_not_taken_reason": _welsh_swab_sample_not_taken_categories,
        "blood_not_taken_reason": _welsh_blood_sample_not_taken_categories,
        "work_status_digital": _welsh_work_status_digital_categories,
        "work_status_employment": _welsh_work_status_employment_categories,
        "work_status_unemployment": _welsh_work_status_unemployment_categories,
        "work_status_education": _welsh_work_status_education_categories,
        "work_sector": _welsh_work_sector_categories,
        "work_location": _welsh_work_location_categories,
        "transport_to_work_or_education": _welsh_transport_to_work_education_categories,
        "ability_to_socially_distance_at_work_or_education": _welsh_ability_to_socially_distance_at_work_or_education_categories,
        "self_isolating_reason_detailed": _welsh_self_isolating_reason_detailed_categories,
        "other_covid_infection_test_results": _welsh_other_covid_infection_test_result_categories,
        "other_antibody_test_results": _welsh_other_covid_infection_test_result_categories,  # TODO Check translation values in test file
    }
    df = apply_value_map_multiple_columns(df, column_editing_map)

    df = translate_column_regex_replace(
        df, "currently_smokes_or_vapes_description", _welsh_currently_smokes_or_vapes_description_categories
    )
    df = translate_column_regex_replace(df, "blood_not_taken_missing_parts", _welsh_blood_kit_missing_categories)
    df = translate_column_regex_replace(
        df, "blood_not_taken_could_not_reason", _welsh_blood_not_taken_reason_categories
    )
    df = translate_column_regex_replace(df, "swab_not_taken_missing_parts", _welsh_swab_kit_missing_categories)

    return df


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

    df = df.withColumn("id", F.concat(F.lit(F.col("participant_id")), F.lit(F.col("participant_completion_window_id"))))
    translation_lookup_df = extract_lookup_csv(lookup_path, validation_schemas["csv_lookup_schema_extended"])
    df = update_from_lookup_df(df, translation_lookup_df, id_column="id")
    df = df.drop("id")

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
    digital_unique_identifiers = ["participant_id", "participant_completion_window_id"]

    digital_free_text_columns = [
        "reason_for_not_consenting_1",
        "reason_for_not_consenting_2",
        "reason_for_not_consenting_3",
        "reason_for_not_consenting_4",
        "reason_for_not_consenting_5",
        "reason_for_not_consenting_6",
        "reason_for_not_consenting_7",
        "reason_for_not_consenting_8",
        "ethnicity_other",
        "swab_not_taken_damage_description",
        "swab_not_taken_other",
        "blood_not_taken_damage_description",
        "blood_not_taken_other",
        "blood_not_taken_could_not_other",
        "work_main_job_title",
        "work_main_job_role",
        "work_sector_other",
        "cis_covid_vaccine_type_other",
    ]
    to_be_translated_df = (
        df.withColumn("id", F.concat(F.lit(F.col("participant_id")), F.lit(F.col("participant_completion_window_id"))))
        .select(digital_unique_identifiers + digital_free_text_columns + ["form_language", "id"])
        .filter(F.col("form_language") == "Welsh")
        .na.drop(how="all", subset=digital_free_text_columns)
        .distinct()
    )
    return to_be_translated_df
