import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from cishouseholds.derive import assign_column_uniform_value
from cishouseholds.derive import assign_column_value_from_multiple_column_map
from cishouseholds.derive import assign_raw_copies
from cishouseholds.edit import apply_value_map_multiple_columns
from cishouseholds.edit import clean_barcode_simple
from cishouseholds.edit import edit_to_sum_or_max_value
from cishouseholds.pipeline.survey_responses_version_2_ETL import transform_survey_responses_generic


def digital_specific_transformations(df: DataFrame) -> DataFrame:
    """
    Call functions to process digital specific variable transformations.
    """
    df = assign_column_uniform_value(df, "survey_response_dataset_major_version", 3)
    df = df.withColumn("visit_id", F.col("participant_completion_window_id"))
    df = df.withColumn(
        "visit_datetime",
        F.coalesce(
            F.col("swab_taken_datetime"),
            F.col("blood_taken_datetime"),
            F.col("survey_completed_datetime"),
            F.col("sample_kit_dispatched_datetime"),
        ),
    )  # Placeholder for 2199

    df = transform_survey_responses_generic(df)
    df = df.withColumn("self_isolating_reason_digital", F.col("self_isolating_reason"))
    column_list = ["work_status_digital", "work_status_employment", "work_status_unemployment", "work_status_education"]
    df = assign_column_value_from_multiple_column_map(
        df,
        "work_status_v2",
        [
            [
                "Employed and currently working",
                [
                    "Employed",
                    "Currently working. This includes if you are on sick or other leave for less than 4 weeks",
                    None,
                    None,
                ],
            ],
            [
                "Self-employed and currently working",
                [
                    "Employed",
                    "Currently not working. This includes if you are on sick or other leave such as maternity or paternity for longer than 4 weeks",  # noqa: E501
                    None,
                    None,
                ],
            ],
            [
                "Self-employed and currently not working",
                [
                    "Self-employed",
                    "Currently working. This includes if you are on sick or other leave for less than 4 weeks",
                    None,
                    None,
                ],
            ],
            [
                "Looking for paid work and able to start",
                [
                    "Self-employed",
                    "Currently not working. This includes if you are on sick or other leave such as maternity or paternity for longer than 4 weeks",  # noqa: E501
                    None,
                    None,
                ],
            ],
            [
                "Not working and not looking for work",
                [
                    "Not in paid work. This includes being unemployed or doing voluntary work",
                    None,
                    "Looking for paid work and able to start",
                    None,
                ],
            ],
            [
                "Retired",
                [
                    "Not in paid work. This includes being unemployed or doing voluntary work",
                    None,
                    "Not looking for paid work. This includes looking after the home or family or not wanting a job or being long-term sick or disabled",  # noqa: E501
                    None,
                ],
            ],
            [
                "Child under 4-5y not attending child care",
                ["Not in paid work. This includes being unemployed or doing voluntary work", None, "Retired", None],
            ],
            [
                "Child under 4-5y attending child care",
                [
                    "Education",
                    None,
                    None,
                    "A child below school age and not attending a nursery or pre-school or childminder",
                ],
            ],
            [
                "4-5y and older at school/home-school",
                [
                    "Education",
                    None,
                    None,
                    "A child below school age and attending a nursery or pre-school or childminder",
                ],
            ],
            [
                "Attending college or FE (including if temporarily absent)",
                [
                    "Education",
                    None,
                    None,
                    ["A child aged 4 or over at school", "A child aged 4 or over at home-school"],
                ],
            ],
            [
                "Attending university (including if temporarily absent)",
                [
                    "Education",
                    None,
                    None,
                    "Attending a college or other further education provider including apprenticeships",
                ],
            ],
            [12, ["Education", None, None, "Attending university"]],
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
                    "Currently not working. This includes if you are on sick or other leave such as maternity or paternity for longer than 4 weeks",  # noqa: E501
                    None,
                    None,
                ],
            ],
            ["Employed", ["Self-employed", None, "Looking for paid work and able to start", None]],
            [
                "Self-employed",
                [
                    "Self-employed",
                    None,
                    "Not looking for paid work. This includes looking after the home or family or not wanting a job or being long-term sick or disabled",  # noqa: E501
                    None,
                ],
            ],
            ["Self-employed", [None, None, None]],
            ["Not working (unemployed, retired, long-term sick etc.)", ["Self-employed", None, None, None]],
            [
                "Not working (unemployed, retired, long-term sick etc.)",
                [
                    "Not in paid work. This includes being unemployed or doing voluntary work",
                    None,
                    "Looking for paid work and able to start",
                    None,
                ],
            ],
            [
                "Not working (unemployed, retired, long-term sick etc.)",
                [
                    "Not in paid work. This includes being unemployed or doing voluntary work",
                    None,
                    "Not looking for paid work. This includes looking after the home or family or not wanting a job or being long-term sick or disabled",  # noqa: E501
                    None,
                ],
            ],
            [
                "Not working (unemployed, retired, long-term sick etc.)",
                ["Not in paid work. This includes being unemployed or doing voluntary work", None, "Retired", None],
            ],
            [
                "Not working (unemployed, retired, long-term sick etc.)",
                ["Not in paid work. This includes being unemployed or doing voluntary work", None, None, None],
            ],
            [
                "Student",
                [
                    "Education",
                    None,
                    None,
                    "A child below school age and not attending a nursery or pre-school or childminder",
                ],
            ],
            [
                "Student",
                [
                    "Education",
                    None,
                    None,
                    "A child below school age and attending a nursery or pre-school or childminder",
                ],
            ],
            ["Student", ["Education", None, None, "A child aged 4 or over at school"]],
            ["Student", ["Education", None, None, "A child aged 4 or over at home-school"]],
            [
                "Student",
                [
                    "Education",
                    None,
                    None,
                    "Attending a college or other further education provider including apprenticeships",
                ],
            ],
        ],
        column_list,
    )
    df = assign_column_value_from_multiple_column_map(
        df,
        "self_isolating_reason",
        [
            ["No", ["No", None]],
            [
                "Yes, you have/have had symptoms",
                ["Yes", "I have or have had symptoms of COVID-19 or a positive test"],
            ],
            [
                "Yes, someone you live with had symptoms",
                [
                    "Yes",
                    "I haven't had any symptoms but I live with someone who has or has had symptoms or a positive test",
                ],
            ],
            [
                "Yes, for other reasons (e.g. going into hospital, quarantining),",  # noqa: E501
                [
                    "Yes",
                    "Due to increased risk of getting COVID-19 such as having been in contact with a known case or quarantining after travel abroad",  # noqa: E501
                ],
            ],
            [
                "Yes, for other reasons (e.g. going into hospital, quarantining),",  # noqa: E501
                ["Yes", "Due to reducing my risk of getting COVID-19 such as going into hospital or shielding"],
            ],
        ],
        ["self_isolating", "self_isolating_reason"],
    )
    df = clean_barcode_simple(df, "swab_sample_barcode_user_entered")
    df = clean_barcode_simple(df, "blood_sample_barcode_user_entered")

    df = df.withColumn("times_outside_shopping_or_socialising_last_7_days", F.lit(None))
    """
    Create copies of all digital specific variables to be remapped
    """
    raw_copy_list = [
        "participant_survey_status",
        "participant_withdrawal_type",
        "survey_response_type",
        "work_sector",
        "illness_reduces_activity_or_ability",
        # "work_location",  # is already made raw in transform_survey_responses_generic
        "ability_to_socially_distance_at_work_or_education",
        "last_covid_contact_type",
        "last_suspected_covid_contact_type",
        "physical_contact_under_18_years",
        "physical_contact_18_to_69_years",
        "physical_contact_over_70_years",
        "social_distance_contact_under_18_years",
        "social_distance_contact_18_to_69_years",
        "social_distance_contact_over_70_years",
        "times_hour_or_longer_another_home_last_7_days",
        "times_hour_or_longer_another_person_your_home_last_7_days",
        "times_shopping_last_7_days",
        "times_socialising_last_7_days",
        "face_covering_work_or_education",
        "face_covering_other_enclosed_places",
        "other_covid_infection_test_results",
        "other_antibody_test_results",
        "cis_covid_vaccine_type",
        "cis_covid_vaccine_number_of_doses",
        "cis_covid_vaccine_type_1",
        "cis_covid_vaccine_type_2",
        "cis_covid_vaccine_type_3",
        "cis_covid_vaccine_type_4",
        "cis_covid_vaccine_type_5",
        "cis_covid_vaccine_type_6",
    ]
    df = assign_raw_copies(df, [column for column in raw_copy_list if column in df.columns])
    """
    Sets categories to map for digital specific variables to Voyager 0/1/2 equivalent
    """
    contact_people_value_map = {
        "1 to 5": "1-5",
        "6 to 10": "6-10",
        "11 to 20": "11-20",
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
    }
    column_editing_map = {
        "participant_survey_status": {"Complete": "Completed"},
        "participant_withdrawal_type": {
            "Withdrawn - no future linkage": "Withdrawn_no_future_linkage",
            "Withdrawn - no future linkage or use of samples": "Withdrawn_no_future_linkage_or_use_of_samples",
        },
        "survey_response_type": {"First Survey": "First Visit", "Follow-up Survey": "Follow-up Visit"},
        "voucher_type_preference": {"Letter": "Paper", "Email": "email_address"},
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
            "Primary care - for example in a GP or dentist": "Yes, in primary care, e.g. GP, dentist",
            "Secondary care - for example in a hospital": "Yes, in secondary care, e.g. hospital",
            "Another type of healthcare - for example mental health services": "Yes, in other healthcare settings, e.g. mental health",  # noqa: E501
        },
        "illness_reduces_activity_or_ability": {
            "Yes a little": "Yes, a little",
            "Yes a lot": "Yes, a lot",
        },
        "work_location": {
            "From home meaning in the same grounds or building as your home": "Working from home",
            "Somewhere else meaning not at your home)": "Working somewhere else (not your home)",
            "Both from home and work somewhere else": "Both (from home and somewhere else)",
        },
        "transport_to_work_or_education": {
            "Bus or minibus or coach": "Bus, minibus, coach",
            "Motorbike or scooter or moped": "Motorbike, scooter or moped",
            "Taxi or minicab": "Taxi/minicab",
            "Underground or Metro or Light Rail or Tram": "Underground, metro, light rail, tram",
        },
        "ability_to_socially_distance_at_work_or_education": {
            "Difficult to maintain 2 metres apart. But you can usually be at least 1 metre away from other people": "Difficult to maintain 2m, but can be 1m",  # noqa: E501
            "Easy to maintain 2 metres apart. It is not a problem to stay this far away from other people": "Easy to maintain 2m",  # noqa: E501
            "Relatively easy to maintain 2 metres apart. Most of the time you can be 2 meters away from other people": "Relatively easy to maintain 2m",  # noqa: E501
            "Very difficult to be more than 1m away as your work means you are in close contact with others on a regular basis": "Very difficult to be more than 1m away",  # noqa: E501
        },
        "last_covid_contact_type": {
            "Someone I live with": "Living in your own home",
            "Someone I do not live with": "Outside your home",
        },
        "last_suspected_covid_contact_type": {
            "Someone I live with": "Living in your own home",
            "Someone I do not live with": "Outside your home",
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
            "I cover my face for other reasons - for example for religious or cultural reasons": "My face is already covered",  # noqa: E501
        },
        "face_covering_other_enclosed_places": {
            "Prefer not to say": None,
            "Yes sometimes": "Yes, sometimes",
            "Yes always": "Yes, always",
            "I am not going to other enclosed public spaces or using public transport": "Not going to other enclosed public spaces or using public transport",  # noqa: E501
            "I cover my face for other reasons - for example for religious or cultural reasons": "My face is already covered",  # noqa: E501
        },
        "other_covid_infection_test_results": {
            "All tests failed": "All Tests failed",
            "One or more tests were negative and none were positive": "Any tests negative, but none positive",
            "One or more tests were positive": "One or more positive test(s)",
        },
        "other_antibody_test_results": {
            "All tests failed": "All Tests failed",
            "One or more tests were negative for antibodies and none were positive": "Any tests negative, but none positive",  # noqa: E501
            "One or more tests were positive for antibodies": "One or more positive test(s)",
        },
        "cis_covid_vaccine_type": vaccine_type_map,
        "cis_covid_vaccine_number_of_doses": {
            "1 dose": "1",
            "2 doses": "2",
            "3 doses": "3 or more",
            "4 doses": "3 or more",
            "5 doses": "3 or more",
            "6 doses or more": "3 or more",
        },
        "cis_covid_vaccine_type_1": vaccine_type_map,
        "cis_covid_vaccine_type_2": vaccine_type_map,
        "cis_covid_vaccine_type_3": vaccine_type_map,
        "cis_covid_vaccine_type_4": vaccine_type_map,
        "cis_covid_vaccine_type_5": vaccine_type_map,
        "cis_covid_vaccine_type_6": vaccine_type_map,
    }
    df = apply_value_map_multiple_columns(df, column_editing_map)

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
    return df
