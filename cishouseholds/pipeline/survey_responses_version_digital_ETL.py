import pyspark.sql.functions as F

from cishouseholds.derive import assign_column_uniform_value
from cishouseholds.derive import assign_column_value_from_multiple_column_map
from cishouseholds.derive import map_options_to_bool_columns
from cishouseholds.edit import clean_barcode_simple
from cishouseholds.edit import edit_to_sum_or_max_value
from cishouseholds.pipeline.survey_responses_version_2_ETL import transform_survey_responses_generic


def digital_specific_transformations(df):
    df = assign_column_uniform_value(df, "survey_response_dataset_major_version", 3)
    df = df.withColumn("visit_id", F.col("participant_completion_window_id"))
    df = df.withColumn("visit_datetime", F.lit(None).cast("timestamp"))  # Placeholder for 2199

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
    df = map_options_to_bool_columns(
        df,
        "currently_smokes_or_vapes_description",
        {
            "cigarettes": "smoke_cigarettes",
            "cigars": "smokes_cigar",
            "pipe": "smokes_pipe",
            "vape/E-cigarettes": "smokes_vape_e_cigarettes",
            "Hookah/shisha pipes": "smokes_hookah_shisha_pipes",
        },
    )
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
    return df
