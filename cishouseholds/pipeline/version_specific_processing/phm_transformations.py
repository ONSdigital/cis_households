# flake8: noqa
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from cishouseholds.derive import assign_column_uniform_value
from cishouseholds.derive import assign_column_value_from_multiple_column_map
from cishouseholds.derive import assign_date_from_filename
from cishouseholds.derive import assign_datetime_from_coalesced_columns_and_log_source
from cishouseholds.derive import assign_raw_copies
from cishouseholds.derive import concat_fields_if_true
from cishouseholds.derive import derive_had_symptom_last_7days_from_digital
from cishouseholds.derive import map_options_to_bool_columns
from cishouseholds.edit import apply_value_map_multiple_columns
from cishouseholds.edit import clean_barcode_simple
from cishouseholds.edit import edit_to_sum_or_max_value
from cishouseholds.edit import survey_edit_auto_complete
from cishouseholds.edit import update_column_in_time_window
from cishouseholds.edit import update_column_values_from_map
from cishouseholds.edit import update_strings_to_sentence_case
from cishouseholds.edit import update_value_if_multiple_and_ref_in_list

# THIS IS A DIRECT COPY OF A DIGITAL VERSION-SPECIFIC TRANSFORMATIONS - NEEDS TO BE UPDATED WITH THE PHM-SPECIFIC TRANSFORMATIONS AND THEN THEN THE REDUNDANT PROCESSES CLEANED UP
# DOUBLE CHECK IF REDUNDANT BITS REMOVED BEFORE MERGING ON
def transform_survey_responses_version_phm_delta(df: DataFrame) -> DataFrame:
    """
    Call functions to process digital specific variable transformations.
    """
    df = assign_column_value_from_multiple_column_map(
        df,
        "self_isolating_reason",
        [
            [
                "No",
                ["No", None],
            ],
            [
                "Yes, you have/have had symptoms",
                ["Yes", "I have or have had symptoms of COVID-19 or a positive test"],
            ],
            [
                "Yes, someone you live with had symptoms",
                [
                    "Yes",
                    [
                        "I haven't had any symptoms but I live with someone who has or has had symptoms or a positive test",
                        # noqa: E501
                        # TODO: Remove once encoding fixed in raw data
                        "I haven&#39;t had any symptoms but I live with someone who has or has had symptoms or a positive test",
                        # noqa: E501
                    ],
                ],
            ],
            [
                "Yes, for other reasons (e.g. going into hospital, quarantining)",
                [
                    "Yes",
                    "Due to increased risk of getting COVID-19 such as having been in contact with a known case or quarantining after travel abroad",
                ],  # noqa: E501
            ],
            [
                "Yes, for other reasons (e.g. going into hospital, quarantining)",
                [
                    "Yes",
                    "Due to reducing my risk of getting COVID-19 such as going into hospital or shielding",
                ],  # noqa: E501
            ],
        ],
        ["self_isolating", "self_isolating_reason_detailed"],
    )

    column_list = ["work_status_digital", "work_status_employment", "work_status_unemployment", "work_status_education"]
    df = assign_column_value_from_multiple_column_map(
        df,
        "work_status_v2",
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
                    "Not looking for paid work. This includes looking after the home or family or not wanting a job or being long-term sick or disabled",
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
                    "Currently not working -  for example on sick or other leave such as maternity or paternity for longer than 4 weeks",
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
                    "Not looking for paid work. This includes looking after the home or family or not wanting a job or being long-term sick or disabled",
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
                    "Currently not working -  for example on sick or other leave such as maternity or paternity for longer than 4 weeks",
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
                    "Not looking for paid work. This includes looking after the home or family or not wanting a job or being long-term sick or disabled",
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

    df = clean_barcode_simple(df, "swab_barcode_corrected")
    df = clean_barcode_simple(df, "blood_barcode_corrected")

    df = map_options_to_bool_columns(
        df,
        "currently_smokes_or_vapes_description",
        {
            "Cigarettes": "smoke_cigarettes",
            "Cigars": "smokes_cigar",
            "Pipe": "smokes_pipe",
            "Vape or E-cigarettes": "smokes_vape_e_cigarettes",
            "Hookah or shisha pipes": "smokes_hookah_shisha_pipes",
        },
        ";",
    )
    df = map_options_to_bool_columns(
        df,
        "blood_not_taken_missing_parts",
        {
            "Small sample test tube. This is the tube that is used to collect the blood.": "blood_not_taken_missing_parts_small_sample_tube",
            # noqa: E501
            "Large sample carrier tube with barcode on. This is the tube that you put the small sample test tube in to after collecting blood.": "blood_not_taken_missing_parts_large_sample_carrier",
            # noqa: E501
            "Re-sealable biohazard bag with absorbent pad": "blood_not_taken_missing_parts_biohazard_bag",
            "Copy of your blood barcode": "blood_not_taken_missing_parts_blood_barcode",
            "Lancets": "blood_not_taken_missing_parts_lancets",
            "Plasters": "blood_not_taken_missing_parts_plasters",
            "Alcohol wipes": "blood_not_taken_missing_parts_alcohol_wipes",
            "Cleansing wipe": "blood_not_taken_missing_parts_cleansing_wipe",
            "Sample box": "blood_not_taken_missing_parts_sample_box",
            "Sample return bag with a return label on": "blood_not_taken_missing_parts_sample_return_bag",
            "Other please specify": "blood_not_taken_missing_parts_other",
        },
        ";",
    )
    df = map_options_to_bool_columns(
        df,
        "blood_not_taken_could_not_reason",
        {
            "I couldn't get enough blood into the pot": "blood_not_taken_could_not_reason_not_enough_blood",
            "The pot spilled": "blood_not_taken_could_not_reason_pot_spilled",
            "I had bruising or pain": "blood_not_taken_could_not_reason_had_bruising",
            "I felt unwell": "blood_not_taken_could_not_reason_unwell",
            "Other please specify": "blood_not_taken_could_not_reason_other",
            "There were issues with the kit": "blood_not_taken_could_not_reason_issues_with_kit",
        },
        ";",
    )
    df = map_options_to_bool_columns(
        df,
        "swab_not_taken_missing_parts",
        {
            "Sample pot with fluid in the bottom and barcode on": "swab_not_taken_missing_parts_sample_pot",
            "Swab stick": "swab_not_taken_missing_parts_swab_stick",
            "Re-sealable biohazard bag with absorbent pad": "swab_not_taken_missing_parts_biohazard_bag",
            "Copy of your swab barcode": "swab_not_taken_missing_parts_swab_barcode",
            "Sample box": "swab_not_taken_missing_parts_sample_box",
            "Sample return bag with a return label on": "swab_not_taken_missing_parts_return_bag",
            "Other please specify": "swab_not_taken_missing_parts_other",
        },
        ";",
    )
    df = map_options_to_bool_columns(
        df,
        "think_have_covid_any_symptom_list_1",
        {
            "Runny nose or sneezing": "think_have_covid_symptom_runny_nose_or_sneezing",
            "Loss of smell": "think_have_covid_symptom_loss_of_smell",
            "Loss of taste": "think_have_covid_symptom_loss_of_taste",
            "Sore throat": "think_have_covid_symptom_sore_throat",
            "Cough": "think_have_covid_symptom_cough",
            "Shortness of breath": "think_have_covid_symptom_shortness_of_breath",
            "Noisy breathing or wheezing": "think_have_covid_symptom_noisy_breathing",
            "Abdominal pain": "think_have_covid_symptom_abdominal_pain",
            "Nausea or vomiting": "think_have_covid_symptom_nausea_or_vomiting",
            "Diarrhoea": "think_have_covid_symptom_diarrhoea",
            "Loss of appetite or eating less than usual": "think_have_covid_symptom_loss_of_appetite",
            "None of these symptoms": "think_have_covid_symptom_none_list_1",
        },
        ";",
    )
    df = map_options_to_bool_columns(
        df,
        "think_have_covid_any_symptom_list_2",
        {
            "Headache": "think_have_covid_symptom_headache",
            "Muscle ache": "think_have_covid_symptom_muscle_ache",
            "Weakness or tiredness": "think_have_covid_symptom_fatigue",
            "Fever including high temperature": "think_have_covid_symptom_fever",
            "More trouble sleeping than usual": "think_have_covid_symptom_more_trouble_sleeping",
            "Memory loss or confusion": "think_have_covid_symptom_memory_loss_or_confusion",
            "Difficulty concentrating": "think_have_covid_symptom_difficulty_concentrating",
            "Worry or anxiety": "think_have_covid_symptom_anxiety",
            "Low mood or not enjoying anything": "think_have_covid_symptom_low_mood",
            "None of these symptoms": "think_have_covid_symptom_none_list_2",
        },
        ";",
    )
    df = df.withColumn(
        "think_have_covid_symptoms",
        F.when(
            (F.col("think_have_covid_symptom_none_list_1") != "None of these symptoms")
            or (F.col("think_have_covid_symptom_none_list_2") != "None of these symptoms"),
            "Yes",
        ).otherwise("No"),
    )

    df = map_options_to_bool_columns(
        df,
        "think_had_covid_any_symptom_list_1",
        {
            "Runny nose or sneezing": "think_had_covid_symptom_runny_nose_or_sneezing",
            "Loss of smell": "think_had_covid_symptom_loss_of_smell",
            "Loss of taste": "think_had_covid_symptom_loss_of_taste",
            "Sore throat": "think_had_covid_symptom_sore_throat",
            "Cough": "think_had_covid_symptom_cough",
            "Shortness of breath": "think_had_covid_symptom_shortness_of_breath",
            "Noisy breathing or wheezing": "think_had_covid_symptom_noisy_breathing",
            "Abdominal pain": "think_had_covid_symptom_abdominal_pain",
            "Nausea or vomiting": "think_had_covid_symptom_nausea_or_vomiting",
            "Diarrhoea": "think_had_covid_symptom_diarrhoea",
            "Loss of appetite or eating less than usual": "think_had_covid_symptom_loss_of_appetite",
            "None of these symptoms": "think_had_covid_symptom_none_list_1",
        },
        ";",
    )
    df = map_options_to_bool_columns(
        df,
        "think_had_covid_any_symptom_list_2",
        {
            "Headache": "think_had_covid_symptom_headache",
            "Muscle ache": "think_had_covid_symptom_muscle_ache",
            "Weakness or tiredness": "think_had_covid_symptom_fatigue",
            "Fever including high temperature": "think_had_covid_symptom_fever",
            "More trouble sleeping than usual": "think_had_covid_symptom_more_trouble_sleeping",
            "Memory loss or confusion": "think_had_covid_symptom_memory_loss_or_confusion",
            "Difficulty concentrating": "think_had_covid_symptom_difficulty_concentrating",
            "Worry or anxiety": "think_had_covid_symptom_anxiety",
            "Low mood or not enjoying anything": "think_had_covid_symptom_low_mood",
            "None of these symptoms": "think_had_covid_symptom_none_list_2",
        },
        ";",
    )

    df = df.withColumn(
        "think_had_covid_any_symptoms",
        F.when(
            (F.col("think_had_covid_symptom_none_list_1") != "None of these symptoms")
            or (F.col("think_had_covid_symptom_none_list_2") != "None of these symptoms"),
            "Yes",
        ).otherwise("No"),
    )

    df = df.withColumn("times_outside_shopping_or_socialising_last_7_days", F.lit(None))
    raw_copy_list = [
        "participant_survey_status",
        "participant_withdrawal_type",
        "survey_response_type",
        "work_sector",
        "illness_reduces_activity_or_ability",
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
        "cis_covid_vaccine_type_other",
        "cis_covid_vaccine_number_of_doses",
        "cis_covid_vaccine_type_1",
        "cis_covid_vaccine_type_2",
        "cis_covid_vaccine_type_3",
        "cis_covid_vaccine_type_4",
        "cis_covid_vaccine_type_5",
        "cis_covid_vaccine_type_6",
        "cis_covid_vaccine_type_other_1",
        "cis_covid_vaccine_type_other_2",
        "cis_covid_vaccine_type_other_3",
        "cis_covid_vaccine_type_other_4",
        "cis_covid_vaccine_type_other_5",
        "cis_covid_vaccine_type_other_6",
    ]
    df = assign_raw_copies(df, [column for column in raw_copy_list if column in df.columns])
    """
    Sets categories to map for digital specific variables to Voyager 0/1/2 equivalent
    """
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
        "other_antibody_test_results": {
            "All tests failed": "All Tests failed",
            "One or more tests were negative for antibodies and none were positive": "Any tests negative, but none positive",
            # noqa: E501
            "One or more tests were positive for antibodies": "One or more positive test(s)",
        },
        "cis_covid_vaccine_type": vaccine_type_map,
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

    df = df.withColumn("face_covering_outside_of_home", F.lit(None).cast("string"))
    df = concat_fields_if_true(df, "think_had_covid_which_symptoms", "think_had_covid_which_symptom_", "Yes", ";")
    df = concat_fields_if_true(df, "which_symptoms_last_7_days", "think_have_covid_symptom_", "Yes", ";")
    df = concat_fields_if_true(df, "long_covid_symptoms", "think_have_long_covid_symptom_", "Yes", ";")
    df = survey_edit_auto_complete(
        df,
        "survey_completion_status",
        "participant_completion_window_end_datetime",
        "face_covering_other_enclosed_places",
        "file_date",
    )
    df = update_column_values_from_map(
        df,
        "survey_completion_status",
        {
            "In progress": "Partially Completed",
            "Submitted": "Completed",
        },
    )

    df = derive_had_symptom_last_7days_from_digital(
        df,
        "think_have_covid_symptom_any",
        "think_have_covid_symptom_",
        [
            "fever",
            "muscle_ache",
            "fatigue",
            "sore_throat",
            "cough",
            "shortness_of_breath",
            "headache",
            "nausea_or_vomiting",
            "abdominal_pain",
            "diarrhoea",
            "loss_of_taste",
            "loss_of_smell",
        ],
    )

    df = update_value_if_multiple_and_ref_in_list(
        df,
        "swab_consolidation_point_error",
        ["sample_leaked", "sample_uncompleted"],
        "multiple errors sample discarded",
        "multiple errors sample retained",
        ",",
    )

    df = update_value_if_multiple_and_ref_in_list(
        df,
        "blood_consolidation_point_error",
        ["sample_leaked", "sample_uncompleted"],
        "multiple errors sample discarded",
        "multiple errors sample retained",
        ",",
    )
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
    return df

    df = map_options_to_bool_columns(
        df,
        "transport_shared_outside_household_last_28_days",
        {
            "Underground or metro or light rail or tram": "transport_shared_outside_household_last_28_days_underground_metro",
            "Train": "transport_shared_outside_household_last_28_days_train",
            "Bus or minibus or coach": "transport_shared_outside_household_last_28_days_bus_coach",
            "Car or van": "transport_shared_outside_household_last_28_days_car_van",
            "Taxi or minicab": "transport_shared_outside_household_last_28_days_taxi",
            "Plane": "transport_shared_outside_household_last_28_days_plane",
            "Ferry or boat": "transport_shared_outside_household_last_28_days_ferry_boat",
            "Other method": "transport_shared_outside_household_last_28_days_other",
            "I have not used transport shared with people outside of my home for reasons other than travel to work or education": "transport_shared_outside_household_last_28_days_none",
        },
        ";",
    )
