# flake8: noqa
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from cishouseholds.derive import assign_column_uniform_value
from cishouseholds.derive import assign_isin_list
from cishouseholds.derive import assign_raw_copies
from cishouseholds.derive import assign_taken_column
from cishouseholds.edit import apply_value_map_multiple_columns
from cishouseholds.edit import assign_from_map
from cishouseholds.edit import edit_to_sum_or_max_value
from cishouseholds.edit import update_column_values_from_map


def derive_work_status_columns(df: DataFrame) -> DataFrame:
    work_status_dict = {
        "work_status_v0": {
            "5y and older in full-time education": "Student",
            "Attending college or other further education provider (including apprenticeships) (including if temporarily absent)": "Student",
            # noqa: E501
            "Employed and currently not working (e.g. on leave due to the COVID-19 pandemic (furloughed) or sick leave for 4 weeks or longer or maternity/paternity leave)": "Furloughed (temporarily not working)",
            # noqa: E501
            "Self-employed and currently not working (e.g. on leave due to the COVID-19 pandemic (furloughed) or sick leave for 4 weeks or longer or maternity/paternity leave)": "Furloughed (temporarily not working)",
            # noqa: E501
            "Self-employed and currently working (include if on leave or sick leave for less than 4 weeks)": "Self-employed",
            # noqa: E501
            "Employed and currently working (including if on leave or sick leave for less than 4 weeks)": "Employed",
            # noqa: E501
            "4-5y and older at school/home-school (including if temporarily absent)": "Student",  # noqa: E501
            "Not in paid work and not looking for paid work (include doing voluntary work here)": "Not working (unemployed, retired, long-term sick etc.)",
            # noqa: E501
            "Not working and not looking for work (including voluntary work)": "Not working (unemployed, retired, long-term sick etc.)",
            "Retired (include doing voluntary work here)": "Not working (unemployed, retired, long-term sick etc.)",
            # noqa: E501
            "Looking for paid work and able to start": "Not working (unemployed, retired, long-term sick etc.)",
            # noqa: E501
            "Child under 4-5y not attending nursery or pre-school or childminder": "Student",  # noqa: E501
            "Self-employed and currently not working (e.g. on leave due to the COVID-19 pandemic or sick leave for 4 weeks or longer or maternity/paternity leave)": "Furloughed (temporarily not working)",
            # noqa: E501
            "Child under 5y attending nursery or pre-school or childminder": "Student",  # noqa: E501
            "Child under 4-5y attending nursery or pre-school or childminder": "Student",  # noqa: E501
            "Retired": "Not working (unemployed, retired, long-term sick etc.)",  # noqa: E501
            "Attending university (including if temporarily absent)": "Student",  # noqa: E501
            "Not working and not looking for work": "Not working (unemployed, retired, long-term sick etc.)",
            # noqa: E501
            "Child under 5y not attending nursery or pre-school or childminder": "Student",  # noqa: E501
        },
        "work_status_v1": {
            "Child under 5y attending child care": "Child under 5y attending child care",  # noqa: E501
            "Child under 5y attending nursery or pre-school or childminder": "Child under 5y attending child care",
            # noqa: E501
            "Child under 4-5y attending nursery or pre-school or childminder": "Child under 5y attending child care",
            # noqa: E501
            "Child under 5y not attending nursery or pre-school or childminder": "Child under 5y not attending child care",
            # noqa: E501
            "Child under 5y not attending child care": "Child under 5y not attending child care",  # noqa: E501
            "Child under 4-5y not attending nursery or pre-school or childminder": "Child under 5y not attending child care",
            # noqa: E501
            "Employed and currently not working (e.g. on leave due to the COVID-19 pandemic (furloughed) or sick leave for 4 weeks or longer or maternity/paternity leave)": "Employed and currently not working",
            # noqa: E501
            "Employed and currently working (including if on leave or sick leave for less than 4 weeks)": "Employed and currently working",
            # noqa: E501
            "Not working and not looking for work (including voluntary work)": "Not working and not looking for work",
            # noqa: E501
            "Not in paid work and not looking for paid work (include doing voluntary work here)": "Not working and not looking for work",
            "Not working and not looking for work": "Not working and not looking for work",  # noqa: E501
            "Self-employed and currently not working (e.g. on leave due to the COVID-19 pandemic (furloughed) or sick leave for 4 weeks or longer or maternity/paternity leave)": "Self-employed and currently not working",
            # noqa: E501
            "Self-employed and currently not working (e.g. on leave due to the COVID-19 pandemic or sick leave for 4 weeks or longer or maternity/paternity leave)": "Self-employed and currently not working",
            # noqa: E501
            "Self-employed and currently working (include if on leave or sick leave for less than 4 weeks)": "Self-employed and currently working",
            # noqa: E501
            "Retired (include doing voluntary work here)": "Retired",  # noqa: E501
            "Looking for paid work and able to start": "Looking for paid work and able to start",  # noqa: E501
            "Attending college or other further education provider (including apprenticeships) (including if temporarily absent)": "5y and older in full-time education",
            # noqa: E501
            "Attending university (including if temporarily absent)": "5y and older in full-time education",
            # noqa: E501
            "4-5y and older at school/home-school (including if temporarily absent)": "5y and older in full-time education",
            # noqa: E501
        },
        "work_status_v2": {
            "Retired (include doing voluntary work here)": "Retired",  # noqa: E501
            "Attending college or other further education provider (including apprenticeships) (including if temporarily absent)": "Attending college or FE (including if temporarily absent)",
            # noqa: E501
            "Attending university (including if temporarily absent)": "Attending university (including if temporarily absent)",
            # noqa: E501
            "Child under 5y attending child care": "Child under 4-5y attending child care",  # noqa: E501
            "Child under 5y attending nursery or pre-school or childminder": "Child under 4-5y attending child care",
            # noqa: E501
            "Child under 4-5y attending nursery or pre-school or childminder": "Child under 4-5y attending child care",
            # noqa: E501
            "Child under 5y not attending nursery or pre-school or childminder": "Child under 4-5y not attending child care",
            # noqa: E501
            "Child under 5y not attending child care": "Child under 4-5y not attending child care",  # noqa: E501
            "Child under 4-5y not attending nursery or pre-school or childminder": "Child under 4-5y not attending child care",
            # noqa: E501
            "4-5y and older at school/home-school (including if temporarily absent)": "4-5y and older at school/home-school",
            # noqa: E501
            "Employed and currently not working (e.g. on leave due to the COVID-19 pandemic (furloughed) or sick leave for 4 weeks or longer or maternity/paternity leave)": "Employed and currently not working",
            # noqa: E501
            "Employed and currently working (including if on leave or sick leave for less than 4 weeks)": "Employed and currently working",
            # noqa: E501
            "Not in paid work and not looking for paid work (include doing voluntary work here)": "Not working and not looking for work",
            # noqa: E501
            "Not working and not looking for work (including voluntary work)": "Not working and not looking for work",
            # noqa: E501
            "Self-employed and currently not working (e.g. on leave due to the COVID-19 pandemic or sick leave for 4 weeks or longer or maternity/paternity leave)": "Self-employed and currently not working",
            # noqa: E501
            "Self-employed and currently not working (e.g. on leave due to the COVID-19 pandemic (furloughed) or sick leave for 4 weeks or longer or maternity/paternity leave)": "Self-employed and currently not working",
            # noqa: E501
            "Self-employed and currently working (include if on leave or sick leave for less than 4 weeks)": "Self-employed and currently working",
            # noqa: E501
            "5y and older in full-time education": "4-5y and older at school/home-school",  # noqa: E501
        },
    }

    column_list = ["work_status_v0", "work_status_v1"]
    for column in column_list:
        df = df.withColumn(column, F.col("work_status_v2"))
        df = update_column_values_from_map(df=df, column_name_to_update=column, map=work_status_dict[column])

    df = update_column_values_from_map(
        df=df, column_name_to_update="work_status_v2", map=work_status_dict["work_status_v2"]
    )
    return df


def clean_survey_responses_version_2(df: DataFrame) -> DataFrame:

    # Map to digital from raw V2 values, before editing them to V1 below
    df = assign_from_map(
        df,
        "self_isolating_reason_detailed",
        "self_isolating_reason",
        {
            "Yes for other reasons (e.g. going into hospital or quarantining)": "Due to increased risk of getting COVID-19 such as having been in contact with a known case or quarantining after travel abroad",
            # noqa: E501
            "Yes for other reasons related to reducing your risk of getting COVID-19 (e.g. going into hospital or shielding)": "Due to reducing my risk of getting COVID-19 such as going into hospital or shielding",
            # noqa: E501
            "Yes for other reasons related to you having had an increased risk of getting COVID-19 (e.g. having been in contact with a known case or quarantining after travel abroad)": "Due to increased risk of getting COVID-19 such as having been in contact with a known case or quarantining after travel abroad",
            # noqa: E501
            "Yes because you live with someone who has/has had symptoms but you haven’t had them yourself": "I haven't had any symptoms but I live with someone who has or has had symptoms or a positive test",
            # noqa: E501
            "Yes because you live with someone who has/has had symptoms or a positive test but you haven’t had symptoms yourself": "I haven't had any symptoms but I live with someone who has or has had symptoms or a positive test",
            # noqa: E501
            "Yes because you live with someone who has/has had symptoms but you haven't had them yourself": "I haven't had any symptoms but I live with someone who has or has had symptoms or a positive test",
            # noqa: E501
            "Yes because you have/have had symptoms of COVID-19": "I have or have had symptoms of COVID-19 or a positive test",
            "Yes because you have/have had symptoms of COVID-19 or a positive test": "I have or have had symptoms of COVID-19 or a positive test",
        },
    )

    v2_times_value_map = {
        "None": 0,
        "1": 1,
        "2": 2,
        "3": 3,
        "4": 4,
        "5": 5,
        "6": 6,
        "7 times or more": 7,
        "Participant Would Not/Could Not Answer": None,
    }
    column_editing_map = {
        "deferred": {"Deferred 1": "Deferred"},
        "work_location": {
            "Work from home (in the same grounds or building as your home)": "Working from home",
            "Working from home (in the same grounds or building as your home)": "Working from home",
            "From home (in the same grounds or building as your home)": "Working from home",
            "Work somewhere else (not your home)": "Working somewhere else (not your home)",
            "Somewhere else (not at your home)": "Working somewhere else (not your home)",
            "Somewhere else (not your home)": "Working somewhere else (not your home)",
            "Both (working from home and working somewhere else)": "Both (from home and somewhere else)",
            "Both (work from home and work somewhere else)": "Both (from home and somewhere else)",
        },
        "times_outside_shopping_or_socialising_last_7_days": v2_times_value_map,
        "times_shopping_last_7_days": v2_times_value_map,
        "times_socialising_last_7_days": v2_times_value_map,
        "times_hour_or_longer_another_home_last_7_days": v2_times_value_map,
        "times_hour_or_longer_another_person_your_home_last_7_days": v2_times_value_map,
        "work_sector": {
            "Social Care": "Social care",
            "Transport (incl. storage or logistic)": "Transport (incl. storage, logistic)",
            "Transport (incl. storage and logistic)": "Transport (incl. storage, logistic)",
            "Transport (incl. storage and logistics)": "Transport (incl. storage, logistic)",
            "Retail Sector (incl. wholesale)": "Retail sector (incl. wholesale)",
            "Hospitality (e.g. hotel or restaurant or cafe)": "Hospitality (e.g. hotel, restaurant)",
            "Food Production and agriculture (incl. farming)": "Food production, agriculture, farming",
            "Food production and agriculture (incl. farming)": "Food production, agriculture, farming",
            "Personal Services (e.g. hairdressers or tattooists)": "Personal services (e.g. hairdressers)",
            "Information technology and communication": "Information technology and communication",
            "Financial services (incl. insurance)": "Financial services incl. insurance",
            "Financial Services (incl. insurance)": "Financial services incl. insurance",
            "Civil Service or Local Government": "Civil service or Local Government",
            "Arts or Entertainment or Recreation": "Arts,Entertainment or Recreation",
            "Art or entertainment or recreation": "Arts,Entertainment or Recreation",
            "Arts or entertainment or recreation": "Arts,Entertainment or Recreation",
            "Other employment sector (specify)": "Other occupation sector",
            "Other occupation sector (specify)": "Other occupation sector",
        },
        "work_health_care_area": {
            "Primary Care (e.g. GP or dentist)": "Primary",
            "Primary care (e.g. GP or dentist)": "Primary",
            "Secondary Care (e.g. hospital)": "Secondary",
            "Secondary care (e.g. hospital.)": "Secondary",
            "Secondary care (e.g. hospital)": "Secondary",
            "Other Healthcare (e.g. mental health)": "Other",
            "Other healthcare (e.g. mental health)": "Other",
            "Participant Would Not/Could Not Answer": None,
            "Primary care for example in a GP or dentist": "Primary",
        },
        "face_covering_outside_of_home": {
            "My face is already covered for other reasons (e.g. religious or cultural reasons)": "My face is already covered",
            "Yes at work/school only": "Yes, at work/school only",
            "Yes in other situations only (including public transport/shops)": "Yes, in other situations only",
            "Yes usually both at work/school and in other situations": "Yes, usually both Work/school/other",
            "Yes in other situations only (including public transport or shops)": "Yes, in other situations only",
            "Yes always": "Yes, always",
            "Yes sometimes": "Yes, sometimes",
        },
        "face_covering_other_enclosed_places": {
            "My face is already covered for other reasons (e.g. religious or cultural reasons)": "My face is already covered",
            "Yes at work/school only": "Yes, at work/school only",
            "Yes in other situations only (including public transport/shops)": "Yes, in other situations only",
            "Yes usually both at work/school and in other situations": "Yes, usually both Work/school/other",
            "Yes always": "Yes, always",
            "Yes sometimes": "Yes, sometimes",
        },
        "face_covering_work_or_education": {
            "My face is already covered for other reasons (e.g. religious or cultural reasons)": "My face is already covered",
            "Yes always": "Yes, always",
            "Yes sometimes": "Yes, sometimes",
        },
        "other_antibody_test_results": {
            "One or more negative tests but none positive": "Any tests negative, but none positive",
            "One or more negative tests but none were positive": "Any tests negative, but none positive",
            "All tests failed": "All Tests failed",
        },
        "other_antibody_test_location": {
            "Private Lab": "Private lab",
            "Home Test": "Home test",
            "In the NHS (e.g. GP or hospital)": "In the NHS (e.g. GP, hospital)",
        },
        "other_covid_infection_test_results": {
            "One or more negative tests but none positive": "Any tests negative, but none positive",
            "One or more negative tests but none were positive": "Any tests negative, but none positive",
            "All tests failed": "All Tests failed",
            "Positive": "One or more positive test(s)",
            "Negative": "Any tests negative, but none positive",
            "Void": "All Tests failed",
        },
        "illness_reduces_activity_or_ability": {
            "Yes a little": "Yes, a little",
            "Yes a lot": "Yes, a lot",
            "Participant Would Not/Could Not Answer": None,
        },
        "participant_visit_status": {"Participant did not attend": "Patient did not attend", "Canceled": "Cancelled"},
        "self_isolating_reason": {
            "Yes for other reasons (e.g. going into hospital or quarantining)": "Yes, for other reasons (e.g. going into hospital, quarantining)",
            "Yes for other reasons related to reducing your risk of getting COVID-19 (e.g. going into hospital or shielding)": "Yes, for other reasons (e.g. going into hospital, quarantining)",
            "Yes for other reasons related to you having had an increased risk of getting COVID-19 (e.g. having been in contact with a known case or quarantining after travel abroad)": "Yes, for other reasons (e.g. going into hospital, quarantining)",
            "Yes because you live with someone who has/has had symptoms but you haven’t had them yourself": "Yes, someone you live with had symptoms",
            "Yes because you live with someone who has/has had symptoms or a positive test but you haven’t had symptoms yourself": "Yes, someone you live with had symptoms",
            "Yes because you live with someone who has/has had symptoms but you haven't had them yourself": "Yes, someone you live with had symptoms",
            "Yes because you have/have had symptoms of COVID-19": "Yes, you have/have had symptoms",
            "Yes because you have/have had symptoms of COVID-19 or a positive test": "Yes, you have/have had symptoms",
        },
        "ability_to_socially_distance_at_work_or_education": {
            "Difficult to maintain 2 meters - but I can usually be at least 1m from other people": "Difficult to maintain 2m, but can be 1m",
            "Difficult to maintain 2m - but you can usually be at least 1m from other people": "Difficult to maintain 2m, but can be 1m",
            "Easy to maintain 2 meters - it is not a problem to stay this far away from other people": "Easy to maintain 2m",
            "Easy to maintain 2m - it is not a problem to stay this far away from other people": "Easy to maintain 2m",
            "Relatively easy to maintain 2 meters - most of the time I can be 2m away from other people": "Relatively easy to maintain 2m",
            "Relatively easy to maintain 2m - most of the time you can be 2m away from other people": "Relatively easy to maintain 2m",
            "Very difficult to be more than 1 meter away as my work means I am in close contact with others on a regular basis": "Very difficult to be more than 1m away",
            "Very difficult to be more than 1m away as your work means you are in close contact with others on a regular basis": "Very difficult to be more than 1m away",
        },
        "transport_to_work_or_education": {
            "Bus or Minibus or Coach": "Bus, minibus, coach",
            "Bus or minibus or coach": "Bus, minibus, coach",
            "Bus": "Bus, minibus, coach",
            "Motorbike or Scooter or Moped": "Motorbike, scooter or moped",
            "Motorbike or scooter or moped": "Motorbike, scooter or moped",
            "Car or Van": "Car or van",
            "Taxi/Minicab": "Taxi/minicab",
            "On Foot": "On foot",
            "Underground or Metro or Light Rail or Tram": "Underground, metro, light rail, tram",
            "Other Method": "Other method",
        },
        "last_covid_contact_type": {
            "In your own household": "Living in your own home",
            "Outside your household": "Outside your home",
        },
        "last_suspected_covid_contact_type": {
            "In your own household": "Living in your own home",
            "Outside your household": "Outside your home",
        },
    }
    df = apply_value_map_multiple_columns(df, column_editing_map)
    df = df.withColumn("deferred", F.when(F.col("deferred").isNull(), "NA").otherwise(F.col("deferred")))

    df = df.withColumn("swab_sample_barcode", F.upper(F.col("swab_sample_barcode")))
    df = df.withColumn("blood_sample_barcode", F.upper(F.col("blood_sample_barcode")))
    return df


def transform_survey_responses_version_2_delta(df: DataFrame) -> DataFrame:
    """
    Transformations that are specific to version 2 survey responses.
    """
    raw_copy_list = ["cis_covid_vaccine_number_of_doses"]

    df = assign_raw_copies(df, [column for column in raw_copy_list if column in df.columns])

    df = assign_taken_column(df=df, column_name_to_assign="swab_taken", reference_column="swab_sample_barcode")
    df = assign_taken_column(df=df, column_name_to_assign="blood_taken", reference_column="blood_sample_barcode")

    df = assign_column_uniform_value(df, "survey_response_dataset_major_version", 2)

    # After editing to V1 values in cleaning
    df = assign_isin_list(
        df=df,
        column_name_to_assign="self_isolating",
        reference_column="self_isolating_reason",
        values_list=[
            "Yes, for other reasons (e.g. going into hospital, quarantining)",
            "Yes, you have/have had symptoms",
            "Yes, someone you live with had symptoms",
        ],
        true_false_values=["Yes", "No"],
    )
    df = edit_to_sum_or_max_value(
        df=df,
        column_name_to_update="times_outside_shopping_or_socialising_last_7_days",
        columns_to_sum=[
            "times_shopping_last_7_days",
            "times_socialising_last_7_days",
        ],
        max_value=7,
    )
    df = derive_work_status_columns(df)
    return df
