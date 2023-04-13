# flake8: noqa
from typing import Any
from typing import Dict

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from cishouseholds.derive import assign_column_uniform_value
from cishouseholds.derive import assign_column_value_from_multiple_column_map
from cishouseholds.derive import assign_columns_from_array
from cishouseholds.derive import assign_completion_status
from cishouseholds.derive import assign_date_from_filename
from cishouseholds.derive import assign_datetime_from_coalesced_columns_and_log_source
from cishouseholds.derive import assign_datetime_from_combined_columns
from cishouseholds.derive import assign_raw_copies
from cishouseholds.derive import assign_survey_completed_status
from cishouseholds.derive import assign_window_status
from cishouseholds.derive import combine_like_array_columns
from cishouseholds.derive import concat_fields_if_true
from cishouseholds.derive import derive_had_symptom_last_7days_from_digital
from cishouseholds.derive import map_options_to_bool_columns
from cishouseholds.edit import add_prefix
from cishouseholds.edit import apply_value_map_multiple_columns
from cishouseholds.edit import clean_barcode_simple
from cishouseholds.edit import edit_to_sum_or_max_value
from cishouseholds.edit import survey_edit_auto_complete
from cishouseholds.edit import update_column_in_time_window
from cishouseholds.edit import update_column_values_from_map
from cishouseholds.edit import update_strings_to_sentence_case
from cishouseholds.edit import update_value_if_multiple_and_ref_in_list
from cishouseholds.expressions import all_columns_values_in_list
from cishouseholds.expressions import any_column_not_null
from cishouseholds.pipeline.mapping import transformation_maps

# THIS IS A DIRECT COPY OF A DIGITAL VERSION-SPECIFIC TRANSFORMATIONS - NEEDS TO BE UPDATED WITH THE PHM-SPECIFIC TRANSFORMATIONS AND THEN THEN THE REDUNDANT PROCESSES CLEANED UP
# DOUBLE CHECK IF REDUNDANT BITS REMOVED BEFORE MERGING ON
def phm_transformations(df: DataFrame) -> DataFrame:
    """"""
    df = pre_processing(df)
    df = derive_additional_columns(df)
    return df


def pre_processing(df: DataFrame) -> DataFrame:
    """
    Sets categories to map for digital specific variables to Voyager 0/1/2 equivalent
    """
    raw_copy_list = [
        "work_sector",
        "illness_reduces_activity_or_ability",
        "ability_to_socially_distance_at_work_or_education",
        "physical_contact_under_18_years",
        "physical_contact_18_to_69_years",
        "physical_contact_over_70_years",
        "social_distance_contact_under_18_years",
        "social_distance_contact_18_to_69_years",
        "social_distance_contact_over_70_years",
        "times_shopping_last_7_days",
        "times_socialising_last_7_days",
        "face_covering_work_or_education",
        "other_covid_infection_test_results",
        "cis_covid_vaccine_type",
        "cis_covid_vaccine_type_other",
        "cis_covid_vaccine_number_of_doses",
    ]
    df = assign_raw_copies(df, [column for column in raw_copy_list if column in df.columns])

    contact_people_value_map = {
        "1 to 5": "1-5",
        "6 to 10": "6-10",
        "11 to 20": "11-20",
        "21 or more": "21 or more",
        "Don't know": None,
        "Prefer not to say": None,
    }
    times_value_map = {
        "1": 1,
        "2": 2,
        "3": 3,
        "4": 4,
        "5": 5,
        "6": 6,
        "7 times or more": 7,
        "Don't know": None,
        "None": 0,
        "Prefer not to say": None,
    }
    vaccine_type_map = {
        "Pfizer / BioNTech": "Pfizer/BioNTech",
        "Oxford / AstraZeneca": "Oxford/AstraZeneca",
        "Janssen / Johnson&Johnson": "Janssen/Johnson&Johnson",
        "Another vaccine please specify": "Other / specify",
        "I don't know the type": "Don't know type",
        "Or Another vaccine please specify": "Other / specify",  # changed from "Other /specify"
        "I do not know the type": "Don't know type",
        "Or do you not know which one you had?": "Don't know type",
        "I don&#39;t know the type": "Don't know type",
        "I don&amp;#39;t know the type": "Don't know type",
        "I dont know the type": "Don't know type",
        "Janssen / Johnson&amp;Johnson": "Janssen/Johnson&Johnson",
        "Janssen / Johnson&amp;amp;Johnson": "Janssen/Johnson&Johnson",
        "Oxford also known as AstraZeneca": "Oxford/AstraZeneca",
        "Pfizer also known as BioNTech": "Pfizer/BioNTech",
    }
    column_editing_map = {
        "work_status_employment": {
            "Currently working - this includes if you are on sick leave or other leave for less than 4 weeks": "Currently working. This includes if you are on sick or other leave for less than 4 weeks"
        },
        "work_sector": {
            "Social Care": "Social care",
            "Transport. This includes storage and logistics": "Transport (incl. storage, logistic)",
            "Retail sector. This includes wholesale": "Retail sector (incl. wholesale)",
            "Hospitality - for example hotels or restaurants or cafe": "Hospitality (e.g. hotel, restaurant)",
            "Food production and agriculture. This includes farming": "Food production, agriculture, farming",
            "Personal Services - for example hairdressers or tattooists": "Personal services (e.g. hairdressers)",
            "Information technology and communication": "Information technology and communication",
            "Financial services. This includes insurance": "Financial services incl. insurance",
            "Civil Service or Local Government": "Civil service or Local Government",
            "Arts or entertainment or recreation": "Arts,Entertainment or Recreation",
            "Other employment sector please specify": "Other occupation sector",
        },
        "work_health_care_area": {
            "Secondary care for example in a hospital": "Secondary",
            "Another type of healthcare - for example mental health services?": "Other",
            "Primary care - for example in a GP or dentist": "Primary",
            "Yes, in primary care, e.g. GP, dentist": "Primary",
            "Secondary care - for example in a hospital": "Secondary",
            "Another type of healthcare - for example mental health services": "Other",  # noqa: E501
        },
        "illness_reduces_activity_or_ability": {
            "Yes a little": "Yes, a little",
            "Yes a lot": "Yes, a lot",
        },
        "work_location": {
            "From home meaning in the same grounds or building as your home": "Working from home",
            "Somewhere else meaning not at your home": "Working somewhere else (not your home)",
            "Both from home and work somewhere else": "Both (from home and somewhere else)",
        },
        "transport_to_work_or_education": {
            "Bus or minibus or coach": "Bus, minibus, coach",
            "Motorbike or scooter or moped": "Motorbike, scooter or moped",
            "Taxi or minicab": "Taxi/minicab",
            "Underground or Metro or Light Rail or Tram": "Underground, metro, light rail, tram",
        },
        "ability_to_socially_distance_at_work_or_education": {
            "Difficult to maintain 2 metres apart. But you can usually be at least 1 metre away from other people": "Difficult to maintain 2m, but can be 1m",
            # noqa: E501
            "Easy to maintain 2 metres apart. It is not a problem to stay this far away from other people": "Easy to maintain 2m",
            # noqa: E501
            "Relatively easy to maintain 2 metres apart. Most of the time you can be 2 meters away from other people": "Relatively easy to maintain 2m",
            # noqa: E501
            "Very difficult to be more than 1 metre away. Your work means you are in close contact with others on a regular basis": "Very difficult to be more than 1m away",
        },
        "physical_contact_under_18_years": contact_people_value_map,
        "physical_contact_18_to_69_years": contact_people_value_map,
        "physical_contact_over_70_years": contact_people_value_map,
        "social_distance_contact_under_18_years": contact_people_value_map,
        "social_distance_contact_18_to_69_years": contact_people_value_map,
        "social_distance_contact_over_70_years": contact_people_value_map,
        "times_hour_or_longer_another_home_last_7_days": times_value_map,
        "times_hour_or_longer_another_person_your_home_last_7_days": times_value_map,
        "times_shopping_last_7_days": times_value_map,
        "times_socialising_last_7_days": times_value_map,
        "face_covering_work_or_education": {
            "Prefer not to say": None,
            "Yes sometimes": "Yes, sometimes",
            "Yes always": "Yes, always",
            "I am not going to my place of work or education": "Not going to place of work or education",
            "I cover my face for other reasons - for example for religious or cultural reasons": "My face is already covered",
            # noqa: E501
        },
        "face_covering_other_enclosed_places": {
            "Prefer not to say": None,
            "Yes sometimes": "Yes, sometimes",
            "Yes always": "Yes, always",
            "I am not going to other enclosed public spaces or using public transport": "Not going to other enclosed public spaces or using public transport",
            # noqa: E501
            "I cover my face for other reasons - for example for religious or cultural reasons": "My face is already covered",
            # noqa: E501
        },
        "other_covid_infection_test_results": {
            "All tests failed": "All Tests failed",
            "One or more tests were negative and none were positive": "Any tests negative, but none positive",
            "One or more tests were positive": "One or more positive test(s)",
        },
        "cis_covid_vaccine_type": vaccine_type_map,
        "form_language_launch": {
            "en": "English",
            "cy": "Welsh",
        },
        "form_language_submitted": {
            "en": "English",
            "cy": "Welsh",
        },
    }
    df = apply_value_map_multiple_columns(df, column_editing_map)
    df = assign_datetime_from_combined_columns(
        df=df,
        column_name_to_assign="blood_taken_datetime",
        date_column="blood_taken_date",
        hour_column="blood_taken_time_hour",
        minute_column="blood_taken_time_minute",
        am_pm_column="blood_taken_am_pm",
    )
    df = assign_datetime_from_combined_columns(
        df=df,
        column_name_to_assign="swab_taken_datetime",
        date_column="swab_taken_date",
        hour_column="swab_taken_time_hour",
        minute_column="swab_taken_time_minute",
        am_pm_column="swab_taken_am_pm",
    )

    df = assign_column_uniform_value(df, "survey_response_dataset_major_version", 4)
    # df = generic_processing(df)
    df = df.withColumn(
        "file_date", F.col("survey_completed_datetime")
    )  # the json files dont have dates so we add it here
    # df = assign_completion_status(df=df, column_name_to_assign="survey_completion_status")
    df = add_prefix(df, column_name_to_update="blood_sample_barcode_user_entered", prefix="BLT")
    df = add_prefix(df, column_name_to_update="swab_sample_barcode_user_entered", prefix="SWT")
    return df


def derive_additional_columns(df: DataFrame) -> DataFrame:
    """
    New columns:
    - think_have_covid_any_symptoms
    - think_have_any_symptoms_new_or_worse
    - think_have_long_covid_any_symptoms
    - think_had_covid_any_symptoms
    - think_had_flu_any_symptoms
    - think_had_other_infection_any_symptoms
    - think_have_covid_any_symptom_list
    - think_have_symptoms_new_or_worse_list
    - think_have_long_covid_symptom_list
    - think_had_covid_any_symptom_list
    - think_had_other_infection_symptom_list
    - think_had_flu_symptom_list
    - work_status_v2
    - work_status_v1
    - work_status_v0
    - swab_sample_barcode_user_entered
    - blood_sample_barcode_user_entered
    - times_outside_shopping_or_socialising_last_7_days
    - face_covering_outside_of_home
    - cis_covid_vaccine_number_of_doses
    - visit_datetime
    - from_date

    Reference columns:
    - currently_smokes_or_vapes_description
    - blood_not_taken_could_not_reason
    - transport_shared_outside_household_last_28_days
    - phm_think_had_respiratory_infection_type
    - think_have_covid_any_symptom_list_1
    - think_have_covid_any_symptom_list_2
    - think_have_symptoms_new_or_worse_list_1
    - think_have_symptoms_new_or_worse_list_2
    - think_have_long_covid_symptom_list_1
    - think_have_long_covid_symptom_list_2
    - think_have_long_covid_symptom_list_3
    - think_had_covid_any_symptom_list_1
    - think_had_covid_any_symptom_list_2
    - think_had_other_infection_symptom_list_1
    - think_had_other_infection_symptom_list_2
    - think_had_flu_symptom_list_1
    - think_had_flu_symptom_list_2

    Edited columns:
    - work_not_from_home_days_per_week
    - phm_covid_vaccine_number_of_doses
    """
    df = df.withColumn(
        "visit_datetime",
        F.when(
            F.col("survey_completion_status_flushed") == True,
            F.to_timestamp(F.col("participant_completion_window_end_date"), format="yyyy-MM-dd"),
        ).otherwise(F.to_timestamp(F.col("survey_completed_datetime"), format="yyyy-MM-dd HH:mm:ss")),
    )

    df = assign_date_from_filename(df, "file_date", "survey_response_source_file")

    # df = split_array_columns(df)
    map_to_bool_columns_dict = {
        "currently_smokes_or_vapes_description": "",
        "blood_not_taken_could_not_reason": "",
        "transport_shared_outside_household_last_28_days": "",
        "phm_think_had_respiratory_infection_type": "",
        "think_have_covid_any_symptom_list_1": "think_have_covid",
        "think_have_covid_any_symptom_list_2": "think_have_covid",
        "think_have_any_symptom_new_or_worse_list_1": "think_have_symptoms",
        "think_have_any_symptom_new_or_worse_list_2": "think_have_symptoms",
        "think_had_covid_any_symptom_list_1": "think_had_covid",
        "think_had_covid_any_symptom_list_2": "think_had_covid",
        "think_had_other_infection_any_symptom_list_1": "think_had_other_infection",
        "think_had_other_infection_any_symptom_list_2": "think_had_other_infection",
        "think_had_flu_any_symptom_list_1": "think_had_flu",
        "think_had_flu_any_symptom_list_2": "think_had_flu",
        "think_have_long_covid_any_symptom_list_1": "think_have_long_covid",
        "think_have_long_covid_any_symptom_list_2": "think_have_long_covid",
        "think_have_long_covid_any_symptom_list_3": "think_have_long_covid",
    }
    for col_to_map, prefix in map_to_bool_columns_dict.items():
        if ("symptom" in col_to_map) & ("list_" in col_to_map):
            if "long_covid" in col_to_map:
                dict_to_retrieve = f"long_covid_symptoms_list_{col_to_map[-1:]}"
            else:
                dict_to_retrieve = f"symptoms_list_{col_to_map[-1:]}"
            value_column_map = {key: prefix + value for key, value in transformation_maps[dict_to_retrieve].items()}
        else:
            value_column_map = transformation_maps[col_to_map]
        df = df.withColumn(col_to_map, F.regexp_replace(col_to_map, r"[^a-zA-Z0-9\^,\- ]", "")).withColumn(
            col_to_map, F.regexp_replace(col_to_map, r", ", ";")
        )
        df = map_options_to_bool_columns(df, col_to_map, value_column_map, ";")

    df = assign_any_symptoms(df)

    column_list = ["work_status_digital", "work_status_employment", "work_status_unemployment", "work_status_education"]
    df = assign_column_value_from_multiple_column_map(
        df,
        "work_status_v2",
        [
            [
                "Employed and currently working",
                [
                    "Employed",
                    [
                        "Currently not working due to sickness lasting 4 weeks or more",
                        "Currently not working for other reasons such as maternity or paternity lasting 4 weeks or more",
                    ],
                    None,
                    None,
                ],
            ],
            [
                "Employed and currently not working",
                [
                    "Employed",
                    [
                        "Currently not working -  for example on sick or other leave such as maternity or paternity for longer than 4 weeks",
                        # noqa: E501
                        "Or currently not working -  for example on sick or other leave such as maternity or paternity for longer than 4 weeks?",
                        # noqa: E501
                    ],
                    None,
                    None,
                ],
            ],
            [
                "Self-employed and currently working",
                [
                    "Self-employed",
                    "Currently working. This includes if you are on sick or other leave for less than 4 weeks",
                    None,
                    None,
                ],
            ],
            [
                "Self-employed and currently not working",
                [
                    "Self-employed",
                    [
                        "Currently not working -  for example on sick or other leave such as maternity or paternity for longer than 4 weeks",
                        # noqa: E501
                        "Or currently not working -  for example on sick or other leave such as maternity or paternity for longer than 4 weeks?",
                        # noqa: E501
                    ],
                    None,
                    None,
                ],
            ],
            [
                "Looking for paid work and able to start",
                [
                    "Not in paid work. This includes being unemployed or retired or doing voluntary work",
                    None,
                    "Looking for paid work and able to start",
                    None,
                ],
            ],
            [
                "Not working and not looking for work",
                [
                    "Not in paid work. This includes being unemployed or retired or doing voluntary work",
                    None,
                    [
                        "Not looking for paid work due to long-term sickness or disability",
                        "Not looking for paid work for reasons such as looking after the home or family or not wanting a job",
                    ],
                    # noqa: E501
                    None,
                ],
            ],
            [
                "Retired",
                [
                    "Not in paid work. This includes being unemployed or retired or doing voluntary work",
                    None,
                    ["Retired", "Or retired?"],
                    None,
                ],
            ],
            [
                "Child under 4-5y not attending child care",
                [
                    ["In education", None],
                    None,
                    None,
                    "A child below school age and not attending a nursery or pre-school or childminder",
                ],
            ],
            [
                "Child under 4-5y attending child care",
                [
                    ["In education", None],
                    None,
                    None,
                    "A child below school age and attending a nursery or a pre-school or childminder",
                ],
            ],
            [
                "4-5y and older at school/home-school",
                [
                    ["In education", None],
                    None,
                    None,
                    ["A child aged 4 or over at school", "A child aged 4 or over at home-school"],
                ],
            ],
            [
                "Attending college or FE (including if temporarily absent)",
                [
                    ["In education", None],
                    None,
                    None,
                    "Attending a college or other further education provider including apprenticeships",
                ],
            ],
            [
                "Attending university (including if temporarily absent)",
                [
                    ["In education", None],
                    None,
                    None,
                    ["Attending university", "Or attending university?"],
                ],
            ],
        ],
        column_list,
    )
    df = assign_column_value_from_multiple_column_map(
        df,
        "work_status_v1",
        [
            [
                "Employed and currently working",
                [
                    "Employed",
                    "Currently working. This includes if you are on sick or other leave for less than 4 weeks",
                    None,
                    None,
                ],
            ],
            [
                "Employed and currently not working",
                [
                    "Employed",
                    [
                        "Currently not working due to sickness lasting 4 weeks or more",
                        "Currently not working for other reasons such as maternity or paternity lasting 4 weeks or more",
                    ],
                    # noqa: E501
                    None,
                    None,
                ],
            ],
            [
                "Self-employed and currently working",
                [
                    "Self-employed",
                    "Currently working. This includes if you are on sick or other leave for less than 4 weeks",
                    None,
                    None,
                ],
            ],
            [
                "Self-employed and currently not working",
                [
                    "Self-employed",
                    "Currently not working -  for example on sick or other leave such as maternity or paternity for longer than 4 weeks",
                    None,
                    None,
                ],
            ],
            [
                "Looking for paid work and able to start",
                [
                    "Not in paid work. This includes being unemployed or retired or doing voluntary work",
                    None,
                    "Looking for paid work and able to start",
                    None,
                ],
            ],
            [
                "Not working and not looking for work",
                [
                    "Not in paid work. This includes being unemployed or retired or doing voluntary work",
                    None,
                    [
                        "Not looking for paid work due to long-term sickness or disability",
                        "Not looking for paid work for reasons such as looking after the home or family or not wanting a job",
                    ],
                    None,
                ],
            ],
            [
                "Retired",
                [
                    "Not in paid work. This includes being unemployed or retired or doing voluntary work",
                    None,
                    ["Or retired?", "Retired"],
                    None,
                ],
            ],
            [
                "Child under 5y not attending child care",
                [
                    ["In education", None],
                    None,
                    None,
                    "A child below school age and not attending a nursery or pre-school or childminder",
                ],
            ],
            [
                "Child under 5y attending child care",
                [
                    ["In education", None],
                    None,
                    None,
                    "A child below school age and attending a nursery or a pre-school or childminder",
                ],
            ],
            [
                "5y and older in full-time education",
                [
                    ["In education", None],
                    None,
                    None,
                    [
                        "A child aged 4 or over at school",
                        "A child aged 4 or over at home-school",
                        "Attending a college or other further education provider including apprenticeships",
                        "Attending university",
                    ],
                ],
            ],
        ],
        column_list,
    )
    df = assign_column_value_from_multiple_column_map(
        df,
        "work_status_v0",
        [
            [
                "Employed",
                [
                    "Employed",
                    "Currently working. This includes if you are on sick or other leave for less than 4 weeks",
                    None,
                    None,
                ],
            ],
            [
                "Not working (unemployed, retired, long-term sick etc.)",
                [
                    "Employed",
                    [
                        "Currently not working due to sickness lasting 4 weeks or more",
                        "Currently not working for other reasons such as maternity or paternity lasting 4 weeks or more",
                    ],
                    # noqa: E501
                    None,
                    None,
                ],
            ],
            ["Employed", ["Employed", None, None, None]],
            [
                "Self-employed",
                [
                    "Self-employed",
                    "Currently working. This includes if you are on sick or other leave for less than 4 weeks",
                    None,
                    None,
                ],
            ],
            ["Self-employed", ["Self-employed", None, None, None]],
            [
                "Not working (unemployed, retired, long-term sick etc.)",
                [
                    "Self-employed",
                    "Currently not working -  for example on sick or other leave such as maternity or paternity for longer than 4 weeks",
                    # noqa: E501,
                    None,
                    None,
                ],
            ],
            [
                "Not working (unemployed, retired, long-term sick etc.)",
                [
                    "Not in paid work. This includes being unemployed or retired or doing voluntary work",
                    None,
                    "Looking for paid work and able to start",
                    None,
                ],
            ],
            [
                "Not working (unemployed, retired, long-term sick etc.)",
                [
                    "Not in paid work. This includes being unemployed or retired or doing voluntary work",
                    None,
                    [
                        "Not looking for paid work due to long-term sickness or disability",
                        "Not looking for paid work for reasons such as looking after the home or family or not wanting a job",
                    ],
                    # noqa: E501
                    None,
                ],
            ],
            [
                "Not working (unemployed, retired, long-term sick etc.)",
                [
                    "Not in paid work. This includes being unemployed or retired or doing voluntary work",
                    None,
                    ["Retired", "Or retired?"],
                    None,
                ],
            ],
            [
                "Not working (unemployed, retired, long-term sick etc.)",
                [
                    "Not in paid work. This includes being unemployed or retired or doing voluntary work",
                    None,
                    None,
                    None,
                ],
            ],
            [
                "Student",
                [
                    ["In education", None],
                    None,
                    None,
                    [
                        "A child below school age and not attending a nursery or pre-school or childminder",
                        "A child below school age and attending a nursery or a pre-school or childminder",
                        "A child aged 4 or over at school",
                        "A child aged 4 or over at home-school",
                        "Attending a college or other further education provider including apprenticeships",
                        "Attending university",
                    ],
                ],
            ],
            ["Student", ["In education", None, None, None]],
        ],
        column_list,
    )
    df = clean_barcode_simple(df, "swab_sample_barcode_user_entered")
    df = clean_barcode_simple(df, "blood_sample_barcode_user_entered")

    df = df.withColumn("times_outside_shopping_or_socialising_last_7_days", F.lit(None))
    df = edit_to_sum_or_max_value(
        df=df,
        column_name_to_assign="times_outside_shopping_or_socialising_last_7_days",
        columns_to_sum=[
            "times_shopping_last_7_days",
            "times_socialising_last_7_days",
        ],
        max_value=7,
    )
    df = df.withColumn(
        "work_not_from_home_days_per_week",
        F.greatest("work_not_from_home_days_per_week", "education_in_person_days_per_week"),
    )

    df = df.withColumn("face_covering_outside_of_home", F.lit(None).cast("string"))

    df = df.withColumn("cis_covid_vaccine_number_of_doses", F.col("phm_covid_vaccine_number_of_doses"))

    df = update_column_values_from_map(
        df,
        "phm_covid_vaccine_number_of_doses",
        {
            "1 dose": 1,
            "1": 1,
            "2 doses": 2,
            "2": 2,
            "3 doses": 3,
            "3 or more": 3,
            "4 doses": 4,
            "5 doses": 5,
            "6 doses or more": 6,
            "6 doses": 6,
            "7 doses": 7,
            "8 doses or more": 8,
        },
    )
    df = assign_window_status(
        df=df,
        column_name_to_assign="participant_completion_window_status",
        window_start_column="participant_completion_window_start_date",
        window_end_column="participant_completion_window_end_date",
    )

    df = assign_survey_completed_status(
        df=df,
        column_name_to_assign="survey_completion_status",
        survey_completed_datetime_column="survey_completed_datetime",
        survey_flushed_column="survey_completion_status_flushed",
    )
    return df


def assign_any_symptoms(df: DataFrame):
    """
    Reference columns:
    - think_have_covid_no_symptoms_list_1
    - think_have_covid_no_symptoms_list_2
    - think_have_no_symptoms_new_or_worse_list_1
    - think_have_no_symptoms_new_or_worse_list_2
    - think_have_long_covid_no_symptoms_list_1
    - think_have_long_covid_no_symptoms_list_2
    - think_have_long_covid_no_symptoms_list_3
    - think_had_covid_no_symptoms_list_1
    - think_had_covid_no_symptoms_list_2
    - think_had_flu_no_symptoms_list_1
    - think_had_flu_no_symptoms_list_2
    - think_had_other_infection_no_symptoms_list_1
    - think_had_other_infection_no_symptoms_list_2

    New columns:
    - think_have_covid_any_symptoms
    - think_have_any_symptoms_new_or_worse
    - think_have_long_covid_any_symptoms
    - think_had_covid_any_symptoms
    - think_had_flu_any_symptoms
    - think_had_other_infection_any_symptoms
    """
    df = df.withColumn(
        "think_have_covid_any_symptoms",
        F.when(
            (F.col("think_have_covid_no_symptoms_list_1").contains("None of these symptoms"))
            & (F.col("think_have_covid_no_symptoms_list_2").contains("None of these symptoms")),
            "No",
        )
        .when(
            any_column_not_null(["think_have_covid_any_symptom_list_1", "think_have_covid_any_symptom_list_2"]),
            "Yes",
        )
        .otherwise(None),
    )

    df = df.withColumn(
        "think_have_any_symptoms_new_or_worse",
        F.when(
            (F.col("think_have_no_symptoms_new_or_worse_list_1").contains("None of these symptoms"))
            & (F.col("think_have_no_symptoms_new_or_worse_list_1").contains("None of these symptoms")),
            "No",
        )
        .when(
            any_column_not_null(
                ["think_have_any_symptom_new_or_worse_list_1", "think_have_any_symptom_new_or_worse_list_2"]
            ),
            "Yes",
        )
        .otherwise(None),
    )
    df = df.withColumn(
        "think_have_long_covid_any_symptoms",
        F.when(F.col("think_have_long_covid").isNull(), None)
        .when(
            (F.col("think_have_long_covid_no_symptoms_list_1").contains("None of these symptoms"))
            & (F.col("think_have_long_covid_no_symptoms_list_2").contains("None of these symptoms"))
            & (F.col("think_have_long_covid_no_symptoms_list_3").contains("None of these symptoms")),
            "No",
        )
        .when(
            any_column_not_null(
                [
                    "think_have_long_covid_any_symptom_list_1",
                    "think_have_long_covid_any_symptom_list_2",
                    "think_have_long_covid_any_symptom_list_3",
                ]
            ),
            "Yes",
        )
        .otherwise(None),
    )

    df = df.withColumn(
        "think_had_covid_any_symptoms",
        F.when(F.col("phm_think_had_covid").isNull(), None)
        .when(
            (F.col("think_had_covid_no_symptoms_list_1").contains("None of these symptoms"))
            & (F.col("think_had_covid_no_symptoms_list_2").contains("None of these symptoms")),
            "No",
        )
        .when(any_column_not_null(["think_had_covid_any_symptom_list_1", "think_had_covid_any_symptom_list_2"]), "Yes")
        .otherwise(None),
    )

    df = df.withColumn(
        "think_had_flu_any_symptoms",
        F.when(F.col("phm_think_had_flu").isNull(), None)
        .when(
            (F.col("think_had_flu_no_symptoms_list_1").contains("None of these symptoms"))
            & (F.col("think_had_flu_no_symptoms_list_2").contains("None of these symptoms")),
            "No",
        )
        .when(any_column_not_null(["think_had_flu_any_symptom_list_1", "think_had_flu_any_symptom_list_2"]), "Yes")
        .otherwise(None),
    )
    df = df.withColumn(
        "think_had_other_infection_any_symptoms",
        F.when(F.col("phm_think_had_other_infection").isNull(), None)
        .when(
            (F.col("think_had_other_infection_no_symptoms_list_1").contains("None of these symptoms"))
            & (F.col("think_had_other_infection_no_symptoms_list_2").contains("None of these symptoms")),
            "No",
        )
        .when(
            any_column_not_null(
                ["think_had_other_infection_any_symptom_list_1", "think_had_other_infection_any_symptom_list_2"]
            ),
            "Yes",
        )
        .otherwise(None),
    )
    return df


def split_array_columns(df: DataFrame):
    """"""
    array_column_prefixes = [
        "think_have_covid_any_symptom_list",
        "think_have_symptoms_new_or_worse_list",
        "think_have_long_covid_symptom_list",
        "think_had_covid_any_symptom_list",
        "think_had_flu_symptom_list",
        "think_had_other_infection_symptom_list",
    ]

    array_columns = [
        *array_column_prefixes,
        "currently_smokes_or_vapes_description",
        "phm_think_had_respiratory_infection_type",
    ]

    prefixes = {"currently_smokes_or_vapes_description": "smokes"}

    for prefix in array_column_prefixes:
        df = combine_like_array_columns(df, prefix)
    df.cache()
    for col in array_columns:
        df = assign_columns_from_array(
            df=df,
            array_column_name=col,
            prefix=prefixes.get(col, col.split("_list")[0]),
            true_false_values=["Yes", "No"],
        )
    df.unpersist()

    # remove any columns generated above that refer to the absence of a symptom
    cols = [col for col in df.columns if "none_of_these" in col]
    df = df.drop(*cols)

    return df
