import pytest
from chispa import assert_df_equality

from cishouseholds.edit import update_think_have_covid_symptom_any


def test_match_type(spark_session):
    schema = """
        id integer,
        think_have_covid_symptom_fever string,
        think_have_covid_symptom_muscle_ache string,
        think_have_covid_symptom_fatigue string,
        survey_completion_status string,
        survey_response_dataset_major_version integer,
        think_have_covid_symptom_any string
    """

    input_df = spark_session.createDataFrame(
        # fmt: off
        data=[
            (1, None,  None,   None,   "Partially Completed",  3, None), # all null partially completed digital
            (2, None,  None,   None,   "Completed",            3, None), # all null completed digital
            (3, None,  None,   None,   "Archived",             3, None), # all null archived digital
            (4, None,  None,   None,   "Auto Completed",       3, None), # all null archived digital
            (5, "Yes", None,   "Yes",  "Partially Completed",  3, None), # 3 yes partially completed digital
            (6, "Yes", "No",   "Yes",  "Completed",            3, None), # 3 yes completed digital
            (7, "Yes", None,   "Yes",  "Archived",             3, None), # 3 yes archived digital
            (8, "Yes", "No",   "Yes",  "Auto Completed",       3, None), # 3 yes archived digital
            (9, None,  None,   None,   None,                   2, None), # all null non-digital
            (10,"Yes", "No",   "Yes",  None,                   2, "No"), # 3 yes non-digital
        ],
        # fmt: on
        schema=schema,
    )
    expected_df = spark_session.createDataFrame(
        # fmt: off
        data=[
            (1, None,  None,   None,   "Partially Completed",  3, "No"), # all null partially completed digital
            (2, None,  None,   None,   "Completed",            3, "No"), # all null completed digital
            (3, None,  None,   None,   "Archived",             3, None), # all null archived digital
            (4, None,  None,   None,   "Auto Completed",       3, "No"), # all null archived digital
            (5, "Yes", None,   "Yes",  "Partially Completed",  3, "Yes"), # 3 yes partially completed digital
            (6, "Yes", "No",   "Yes",  "Completed",            3, "Yes"), # 3 yes completed digital
            (7, "Yes", None,   "Yes",  "Archived",             3, None), # 3 yes archived digital
            (8, "Yes", "No",   "Yes",  "Auto Completed",       3, "Yes"), # 3 yes archived digital
            (9, None,  None,   None,   None,                   2, None), # all null non-digital
            (10,"Yes", "No",   "Yes",  None,                   2, "No"), # 3 yes non-digital
        ],
        # fmt: on
        schema=schema
    )
    symptom_list = [
        "think_have_covid_symptom_fever",
        "think_have_covid_symptom_muscle_ache",
        "think_have_covid_symptom_fatigue",
    ]
    output_df = update_think_have_covid_symptom_any(input_df, "think_have_covid_symptom_any", symptom_list)
    assert_df_equality(output_df, expected_df)
