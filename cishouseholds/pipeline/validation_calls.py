# from functools import reduce
# from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from cishouseholds.pipeline.mapping import category_maps
from cishouseholds.validate_class import SparkValidate


def validation_calls(SparkVal):
    """
    Suite of custom validation checks to be performed.

    Parameters
    ----------
    SparkVal : object
        Initialised SparkValidate Class object.
    """
    column_calls = {
        "survey_completed_datetime": {
            "between": {
                "lower_bound": {"inclusive": True, "value": F.to_timestamp(F.lit("11/04/2023"), format="dd/MM/yyyy")},
                "upper_bound": {"inclusive": True, "value": F.col("file_date")},
            }
        },
        "age_at_visit": {
            "between": {
                "lower_bound": {"inclusive": True, "value": 2},
                "upper_bound": {"inclusive": True, "value": 105},
                "allow_none": True,
            }
        },
        # "blood_sample_barcode": {
        #     "matches": r"^BLT[0-9]{8}$",
        #     "subset": F.col("survey_response_dataset_major_version") == 3,
        # },
        # "swab_sample_barcode": {
        #     "matches": r"^SWT[0-9]{8}$",
        #     "subset": F.col("survey_response_dataset_major_version") == 3,
        # },
    }

    for col in SparkVal.dataframe.columns:
        if col in category_maps["iqvia_raw_category_map"]:
            column_calls[col] = {"isin": list(category_maps["iqvia_raw_category_map"][col].keys())}

    SparkVal.validate_column(column_calls)

    # vaccine_columns = []
    # for template in ["cis_covid_vaccine_type_{}", "cis_covid_vaccine_type_other_{}", "cis_covid_vaccine_date_{}"]:
    #     for number in range(1, 5):
    #         vaccine_columns.append(template.format(number))

    dataset_calls = {
        "null": {
            "check_columns": [
                "ons_household_id",
                "participant_id",
                "participant_completion_window_start_date",
                "participant_completion_window_id",
                "participant_completion_window_end_date",
                "survey_completion_status_flushed",
            ]
        },
        "duplicated": [
            # check for duplicated rows
            {"check_columns": SparkVal.dataframe.columns},
            # check for multiple entries assigned to unique participant on same date
            {"check_columns": ["participant_id", "survey_completed_datetime"]},
            # check for multiple entries assigned to unique participant on same date on with same completion id
            {"check_columns": ["participant_id", "participant_completion_window_id", "survey_completed_datetime"]},
            # check for uniqueness in participant_completion_window_id
            {"check_columns": ["participant_completion_window_id"]},
        ],
        # "check_all_null_given_condition": [
        #     # {
        #     #     "condition": F.col("work_main_job_changed") != "Yes",
        #     #     "null_columns": [
        #     #         "work_main_job_title",
        #     #         "work_main_job_role",
        #     #         "work_sector",
        #     #         "work_sector_other",
        #     #         "work_location",
        #     #     ],
        #     # },
        #     {
        #         "condition": F.col("survey_response_type") != "First Visit",
        #         "null_columns": vaccine_columns,
        #     },
        # ],
    }

    SparkVal.validate_all_columns_in_df(dataset_calls)

    # Checks that if "cis_covid_vaccine_type" is not "Another vaccine" then "cis_covid_vaccine_type_other" should be null.
    SparkVal.validate_user_defined_logic(
        logic=(
            ((F.col("cis_covid_vaccine_type") == "Another vaccine") & F.col("cis_covid_vaccine_type_other").isNull())
            | (F.col("cis_covid_vaccine_type") != "Another vaccine")
        ),
        error_message="vaccine type other should be null unless vaccine type is 'Another vaccine'",
        columns=["cis_covid_vaccine_type", "cis_covid_vaccine_type_other"],
    )
    # SparkVal.validate_user_defined_logic(
    #     # Checks for responses on face coverings. Raises an error if "face_covering_outside_of_home" is null
    #     # and "face_covering_other_enclosed_places" and "face_covering_work_or_education" are null.
    #     logic=(
    #         (
    #             (
    #                 F.col("face_covering_other_enclosed_places").isNotNull()
    #                 | F.col("face_covering_work_or_education").isNotNull()
    #             )
    #             & (F.col("face_covering_outside_of_home").isNull())
    #         )
    #         | (F.col("face_covering_outside_of_home").isNotNull())
    #     ),
    #     error_message="face covering is null when face covering at work and other places are null",
    #     columns=[
    #         "face_covering_other_enclosed_places",
    #         "face_covering_work_or_education",
    #         "face_covering_outside_of_home",
    #     ],
    # )
    # SparkVal.validate_user_defined_logic(
    #     # Check if sample taken date is within a valid range
    #     logic=(
    #         ((F.col("visit_datetime") <= F.lit(datetime.now())) & (F.col("visit_datetime") >= F.lit("2020/04/26")))
    #         | (
    #             (F.col("visit_datetime") <= F.lit(datetime.now()))
    #             & (F.col("visit_datetime") >= F.lit("2020/04/26"))
    #             & (F.col("samples_taken_datetime").isNull())
    #         )
    #     ),
    #     error_message="sample taken should be within date range",
    #     columns=["visit_datetime", "samples_taken_datetime"],
    # )


def validation_ETL(df: DataFrame, validation_check_failure_column_name: str, duplicate_count_column_name: str):
    """
    Applies custom suite of validation checks.

    Parameters
    ----------
    df : DataFrame
        Dataframe to validate
    validation_check_failure_column_name : str
        Name for error column wherein each of the validation checks results are appended
    duplicate_count_column_name : str
        Column name in which to count duplicates of rows within the dataframe

    Returns
    -------
        If return_failed True, returns a dataframe of valid survey responses, which have passed checks and a datafreme of
        invalid response which have failed. If return_failed==False, returns only the valid survey response dataframe.
    """
    SparkVal = SparkValidate(dataframe=df, error_column_name=validation_check_failure_column_name)
    SparkVal.count_complete_duplicates(duplicate_count_column_name)
    validation_calls(SparkVal)
    return SparkVal.filter(
        selected_errors=[
            "participant_id, visit_datetime, visit_id, ons_household_id should not be null",
            "the date in visit_datetime should be before the date in file_date plus two days when both swab_sample_barcode and blood_sample_barcode are null",  # noqa:E501
        ],
        any=True,
        return_failed=True,
    )
