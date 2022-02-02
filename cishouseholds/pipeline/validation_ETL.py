import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from cishouseholds.validate_class import SparkValidate

# from cishouseholds.pipeline.category_map import category_maps
# from cishouseholds.pipeline.output_variable_name_map import output_name_map


def validation_ETL(df: DataFrame):
    SparkVal = SparkValidate(dataframe=df, error_column_name="ERROR")

    # calls

    # value_checks = {}
    # for col in SparkVal.dataframe.columns:
    #     if output_name_map.get(col, col) in category_maps["iqvia_raw_category_map"]:
    #         value_checks[col] = {"isin": list(category_maps["iqvia_raw_category_map"][output_name_map[col]].keys())}

    # SparkVal.validate_column(value_checks)

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
        "visit_id": {"contains": r"^DHV"},
        "blood_sample_barcode": {"contains": r"(ON([SWCN]0|S2|S7)[0-9]{7})"},
        "swab_sample_barcode": {"contains": r"(ON([SWCN]0|S2|S7)[0-9]{7})"},
    }

    SparkVal.validate_column(column_calls)

    dataset_calls = {
        "null": {"check_columns": ["ons_household_id", "visit_id", "visit_date_string"]},
        "duplicated": [
            {"check_columns": SparkVal.dataframe.columns},
            {"check_columns": ["participant_id", "visit_id", "visit_datetime"]},
            {"check_columns": ["participant_id", "visit_datetime", "participant_visit_status"]},
            {"check_columns": ["visit_id"]},
        ],
        # "valid_vaccination": {
        #     "visit_type_column": "visit_type",
        #     "check_columns": [
        #         "cis_covid_vaccine_type_1",
        #         "cis_covid_vaccine_type_other_1",
        #         "cis_covid_vaccine_date_1",
        #         "cis_covid_vaccine_type_2",
        #         "cis_covid_vaccine_type_other_2",
        #         "cis_covid_vaccine_date_2",
        #         "cis_covid_vaccine_type_3",
        #         "cis_covid_vaccine_type_other_3",
        #         "cis_covid_vaccine_date_3",
        #         "cis_covid_vaccine_type_4",
        #         "cis_covid_vaccine_type_other_4",
        #         "cis_covid_vaccine_date_4",
        #     ],
        # },
    }

    SparkVal.validate(dataset_calls)

    SparkVal.validate_udl(
        logic=(
            F.when(
                (
                    ((F.col("cis_covid_vaccine_type") == "Other") & F.col("cis_covid_vaccine_type_other").isNull())
                    | (F.col("cis_covid_vaccine_type") != "Other")
                ),
                True,
            ).otherwise(False)
        ),
        error_message="Vaccine type other should be null unless vaccine type is 'Other'",
    )

    SparkVal.validate_udl(
        logic=(
            (
                (F.col("work_social_care") == "Yes")
                & (
                    (F.col("work_nursing_or_residential_care_home") == "Yes")
                    | (F.col("work_direct_contact_persons") == "Yes")
                )  # double check work_direct_contact_persons
            )
            | (F.col("work_social_care") == "No")
        ),
        error_message="relationship between socialcare columns",
    )

    SparkVal.validate_udl(
        logic=(
            (F.col("face_covering_other_enclosed_places").isNotNull() | F.col("face_covering_work").isNotNull())
            & (F.col("face_covering_outside_of_home").isNull())
        ),
        error_message="Validate face covering",
    )

    passed_df, failed_df = SparkVal.filter(
        selected_errors=["participant_id, visit_datetime, visit_id, ons_household_id should not be null"],
        any=True,
        return_failed=True,
    )
    return passed_df, failed_df
