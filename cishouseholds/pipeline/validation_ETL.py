import pyspark.sql.functions as F

from cishouseholds.pipeline.load import extract_from_table
from cishouseholds.pipeline.pipeline_stages import register_pipeline_stage
from cishouseholds.validate_class import SparkValidate


@register_pipeline_stage("validation_ETL")
def validation_ETL(**kwargs):
    df = extract_from_table(kwargs["unioned_survey_response_table"])

    SparkVal = SparkValidate(dataframe=df, error_column_name="ERROR")

    # calls

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
        "blood_barcode": {"contains": r"(ON([SWCN]0|S2|S7)[0-9]{7})"},
        "swab_barcode": {"contains": r"(ON([SWCN]0|S2|S7)[0-9]{7})"},
    }

    dataset_calls = {
        "null": {"check_columns": ["ons_household_id", "visit_id", "visit_date_string"]},
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
    SparkVal.validate_unique(
        [
            {"column_list": "all", "error": "rows should be unique"},
            {"column_list": ["participant_id", "visit_id", "visit_datetime"], "error": "these rows should be unique"},
            {"column_list": ["visit_id"], "error": "visit id should be unique"},
        ]
    )
    SparkVal.validate_column(column_calls)
    SparkVal.validate(dataset_calls)
    SparkVal.validate_udl(
        F.when(
            (F.col("covid_vaccine_type") == "Other" & F.col("covid_vaccine_type_other").isNull())
            | (F.col("covid_vaccine_type") != "Other"),
            True,
        ).otherwise(False),
        "Vaccine type other should be null unless vaccine type is 'Other'",
    )

    valid_survey_responses, erroneous_survey_responses = SparkVal.filter("all", True)
