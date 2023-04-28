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
        "visit_datetime": {
            "between": {
                "lower_bound": {"inclusive": True, "value": F.to_timestamp(F.lit("26/04/2020"), format="dd/MM/yyyy")},
                "upper_bound": {"inclusive": True, "value": F.col("file_date") + F.expr("INTERVAL 2 DAYS")},
            }
        },
        "age_at_visit": {
            "between": {
                "lower_bound": {"inclusive": True, "value": 2},
                "upper_bound": {"inclusive": True, "value": 105},
                "allow_none": True,
            }
        },
        "visit_id": {"starts_with": r"DHV"},
        "blood_sample_barcode": {
            "matches": r"^BLT[0-9]{8}$",
            "subset": F.col("survey_response_dataset_major_version") == 3,
        },
        "swab_sample_barcode": {
            "matches": r"^SWT[0-9]{8}$",
            "subset": F.col("survey_response_dataset_major_version") == 3,
        },
    }

    for col in SparkVal.dataframe.columns:
        if col in category_maps["iqvia_raw_category_map"]:
            column_calls[col] = {"isin": list(category_maps["iqvia_raw_category_map"][col].keys())}

    SparkVal.validate_column(column_calls)

    vaccine_columns = []
    for template in ["covid_vaccine_type_{}", "covid_vaccine_type_other_{}", "covid_vaccine_date_{}"]:
        for number in range(1, 5):
            vaccine_columns.append(template.format(number))

    dataset_calls = {
        "null": {"check_columns": ["ons_household_id", "participant_id", "participant_completion_window_start_date"]},
        "duplicated": [
            # check for duplicated rows
            {"check_columns": SparkVal.dataframe.columns},
            # check for multiple entries assigned to unique participant on same date
            {"check_columns": ["participant_id", "visit_datetime"]},
            # check for multiple entries assigned to unique participant on same date on same visit
            {"check_columns": ["participant_id", "visit_id", "visit_datetime"]},
            # check for multiple entries assigned to unique participant on same date with same visit status
            {"check_columns": ["participant_id", "visit_datetime", "participant_visit_status"]},
            # check for uniqueness in visit_id
            {"check_columns": ["visit_id"]},
        ],
        # "valid_file_date": {
        #     "visit_datetime_column": "visit_datetime",
        #     "file_date_column": "file_date",
        #     "swab_barcode_column": "swab_sample_barcode",
        #     "blood_barcode_column": "blood_sample_barcode",
        # },
        "check_all_null_given_condition": [
            {
                "condition": F.col("work_main_job_changed") != "Yes",
                "null_columns": [
                    "work_main_job_title",
                    "work_main_job_role",
                    "work_sector",
                    "work_sector_other",
                    "work_location",
                ],
            },
            {
                "condition": F.col("survey_response_type") != "First Visit",
                "null_columns": vaccine_columns,
            },
        ],
    }

    SparkVal.validate_all_columns_in_df(dataset_calls)

    # Checks that if "covid_vaccine_type" is not "Other / specify" then "covid_vaccine_type_other" should be null.
    SparkVal.validate_user_defined_logic(
        logic=(
            ((F.col("covid_vaccine_type") == "Other / specify") & F.col("covid_vaccine_type_other").isNull())
            | (F.col("covid_vaccine_type") != "Other / specify")
        ),
        error_message="vaccine type other should be null unless vaccine type is 'Other / specify'",
        columns=["covid_vaccine_type", "covid_vaccine_type_other"],
    )

    # Checks that "work_social_care" is set to "Yes" only when respondent has said "Yes" to either "work_nursing_or_residential_care_home"
    # or "work_directly_contact_patients_or_clients".
    SparkVal.validate_user_defined_logic(
        logic=(
            (
                (F.col("work_social_care") == "Yes")
                & (
                    (F.col("work_nursing_or_residential_care_home") == "Yes")
                    | (F.col("work_direct_contact_patients_or_clients") == "Yes")
                )
            )
            | (F.col("work_social_care") == "No")
        ),
        error_message="work social care should be 'No' if not working in care homes or in direct contact",
        columns=[
            "work_social_care",
            "work_nursing_or_residential_care_home",
            "work_direct_contact_patients_or_clients",
        ],
    )

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
