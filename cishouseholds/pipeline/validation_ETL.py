# from functools import reduce
from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from cishouseholds.pipeline.category_map import category_maps
from cishouseholds.validate_class import SparkValidate


def validation_calls(SparkVal):
    column_calls = {
        "visit_datetime": {
            "between": {
                "lower_bound": {"inclusive": True, "value": F.to_timestamp(F.lit("26/04/2020"), format="dd/MM/yyyy")},
                "upper_bound": {
                    "inclusive": True,
                    "value": F.date_add(
                        F.to_timestamp(
                            F.regexp_extract(F.col("survey_response_source_file"), r"\d{8}(?=.csv)", 0),
                            format="yyyyMMdd",
                        ),
                        1,
                    ),
                },
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
        "blood_sample_barcode": {"matches": r"^(ON([SWCN]0|S2|S7)[0-9]{7})$"},
        "swab_sample_barcode": {"matches": r"^(ON([SWCN]0|S2|S7)[0-9]{7})$"},
        "1tot1_swab": {"matches": 1},  # Swab_matches_not_exact
        # "region_code":"not_null"
    }
    for col in SparkVal.dataframe.columns:
        if col in category_maps["iqvia_raw_category_map"]:
            column_calls[col] = {"isin": list(category_maps["iqvia_raw_category_map"][col].keys())}

    SparkVal.validate_column(column_calls)

    vaccine_columns = []
    for template in ["cis_covid_vaccine_type_{}", "cis_covid_vaccine_type_other_{}", "cis_covid_vaccine_date_{}"]:
        for number in range(1, 5):
            vaccine_columns.append(template.format(number))

    dataset_calls = {
        "null": {"check_columns": ["ons_household_id", "visit_id", "visit_date_string"]},
        "duplicated": [
            {"check_columns": SparkVal.dataframe.columns},
            {"check_columns": ["participant_id", "visit_id", "visit_datetime"]},
            {"check_columns": ["participant_id", "visit_datetime", "participant_visit_status"]},
            {"check_columns": ["visit_id"]},
        ],
        "valid_file_date": {
            "visit_date_column": "visit_datetime",
            "filename_column": "survey_response_source_file",
            "swab_barcode_column": "swab_sample_barcode",
            "blood_barcode_column": "blood_sample_barcode",
        },
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

    SparkVal.validate(dataset_calls)

    SparkVal.validate_udl(
        logic=(
            ((F.col("cis_covid_vaccine_type") == "Other / specify") & F.col("cis_covid_vaccine_type_other").isNull())
            | (F.col("cis_covid_vaccine_type") != "Other / specify")
        ),
        error_message="cis vaccine type other should be null unless vaccine type is 'Other / specify'",
        columns=["cis_covid_vaccine_type", "cis_covid_vaccine_type_other"],
    )

    SparkVal.validate_udl(
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

    SparkVal.validate_udl(
        logic=(
            (
                (
                    F.col("face_covering_other_enclosed_places").isNotNull()
                    | F.col("face_covering_work_or_education").isNotNull()
                )
                & (F.col("face_covering_outside_of_home").isNull())
            )
            | (F.col("face_covering_outside_of_home").isNotNull())
        ),
        error_message="face covering is null when face covering at work and other places are null",
        columns=[
            "face_covering_other_enclosed_places",
            "face_covering_work_or_education",
            "face_covering_outside_of_home",
        ],
    )

    SparkVal.validate_udl(  # Sample_taken_out_of_range
        logic=(
            ((F.col("visit_datetime") <= F.lit(datetime.now())) & (F.col("visit_datetime") >= F.lit("2020/04/26")))
            | (
                (F.col("visit_datetime") <= F.lit(datetime.now()))
                & (F.col("visit_datetime") >= F.lit("2020/04/26"))
                & (F.col("samples_taken_datetime").isNull())
            )
        ),
        error_message="sample taken should be within date range",
        columns=["visit_datetime", "samples_taken_datetime"],
    )

    SparkVal.validate_udl(  # No_blood_match_mismatching
        logic=(
            ((F.datediff(F.col("blood_sample_received_date"), F.col("visit_datetime")) <= 2))
            | ((F.datediff(F.col("blood_sample_received_date"), F.col("visit_datetime")) >= 11))
        ),
        error_message="blood sample recieved date should be between 2 days before and 11 after visit date",
        columns=["blood_sample_received_date", "visit_datetime"],
    )

    SparkVal.validate_udl(
        logic=((F.col("pcr_result_recorded_datetime") >= F.col("visit_datetime")) & (F.col("diff_vs_visit_hr") <= 240)),
        error_message="the swab result should be recieved within 10 days after the visit",
        columns=["pcr_result_recorded_datetime", "visit_datetime", "diff_vs_visit_hr"],
    )


def validation_ETL(df: DataFrame, validation_check_failure_column_name: str, duplicate_count_column_name: str):
    SparkVal = SparkValidate(dataframe=df, error_column_name=validation_check_failure_column_name)
    SparkVal.count_complete_duplicates(duplicate_count_column_name)
    validation_calls(SparkVal)
    return SparkVal.filter(
        selected_errors=[
            "participant_id, visit_datetime, visit_id, ons_household_id should not be null",
            "the date in visit_datetime should be before the date expressed in survey_response_source_file when both swab_sample_barcode_column and blood_sample_barcode_column are null",  # noqa:E501
        ],
        any=True,
        return_failed=True,
    )
