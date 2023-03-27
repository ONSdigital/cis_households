import pytest
from chispa import assert_df_equality

from cishouseholds.pipeline.covid_event_transformations import clean_inconsistent_event_detail_part_1

# fmt: off

@pytest.mark.parametrize(
    ("input_data","expected_data"),
    (
        # missing_other_covid_infection_test
        (
            [
                ("A",   "No",   "One or more positive test(s)", 0,      "No",   "No",   "No",   1),  # set other_covid_infection_test to "Yes" as other_covid_infection_test_results == "Yes"
                ("B",   None,   None,       None,   None,   None,   None,   1),  # think had covid set to yes as a date exists  where covid was onset
                (None,  None,   None,       None,   None,   None,   None,   1),  # do nothing
                ("C",   None,   "One or more positive test(s)", 0,      None,   None,   None,   1),  # set other_covid_infection_test to "Yes" as other_covid_infection_test_results == "Yes"
                ("D",   "No",   "One or more positive test(s)", 0,      "Yes",  "Yes",  "No",   1),  # set other_covid_infection_test to "Yes" as think_had_covid_contacted_nhs =="Yes" and think_had_covid_admitted_to_hopsital == "Yes"
            ]
        ,
            [
                ("A",   "Yes",  "One or more positive test(s)", 0,      "No",   "No",   "Yes",   1),  # set other_covid_infection_test to "Yes" as other_covid_infection_test_results == "Yes"
                ("B",   None,   None,       None,   None,   None,   "Yes",   1),  # think had covid set to yes as a date exists  where covid was onset
                (None,  None,   None,       None,   None,   None,   None,    1),  # do nothing
                ("C",   "Yes",  "One or more positive test(s)", 0,      None,   None,   "Yes",   1),  # set other_covid_infection_test to "Yes" as other_covid_infection_test_results == "Yes"
                ("D",   "Yes",  "One or more positive test(s)", 0,      "Yes",  "Yes",  "Yes",   1),  # set other_covid_infection_test to "Yes" as think_had_covid_contacted_nhs =="Yes" and think_had_covid_admitted_to_hopsital == "Yes"
            ]
        ),
        # erroneous_other_covid_infection_test_results
        (
            [
                ("E",   "No",   "Any tests negative, but none positive", 0,      "No",   "No",   "No",   0),  # do nothing as there has been a 'think_had_covid_onset_date'
                (None,  "No",   "Any tests negative, but none positive", 1,      "No",   "Yes",  "Yes",  0),  # do nothing as there is a symptom count greater than 0
                (None,  "No",   "One or more positive test(s)", 0,      "No",   "Yes",  "No",   0),  # do nothing as other_covid_infection_test_results is 'Positive'
                (None,  "No",   "Any tests negative, but none positive", 0,      "No",   "No",   "No",   0),  # set other_covid_infection_test_results to Null as there is no think_had_covid_onset_date
            ]
        ,
            [
                ("E",   "Yes",  "Any tests negative, but none positive", 0,      "No",   "No",   "Yes",  0),  # do nothing as there has been a 'think_had_covid_onset_date'
                (None,  "Yes",  "Any tests negative, but none positive", 1,      "No",   "Yes",  "Yes",  0),  # 'other_covid_infection_test' set to true as there is evidence the participant had a covid test
                (None,  "No",   "One or more positive test(s)", 0,      "No",   "Yes",  "No",   0),  # do nothing as other_covid_infection_test_results is 'Positive'
                (None,  None,   None,       0,      None,   None,   "No",   0),  # set other_covid_infection_test_results to Null as there is no think_had_covid_onset_date
            ]
        ),
        (
            [
                (None,  "No",   "Any tests negative, but none positive", 0,      "No",   "No",   "No",   1),  # set think_had_covid_contacted_nhs and think_had_covid_admitted_to_hopsital to Null as the flag passed
                (None,  "No",   None,       1,      "No",   "No",   "No",   1),  # do nothing as there is a symptom count greater than 0
            ]

        ,
            [
                (None,  "No",   None,       0,      None,   None,  "No",    1),  # set think_had_covid_contacted_nhs and think_had_covid_admitted_to_hopsital to Null as the flag passed
                (None,  "No",   None,       1,      "No",   "No",  "Yes",   1),  # do nothing
            ]
        ),
        (
            [
                (None,  "No",   "Any tests negative, but none positive", 0,      "No",   "No",   "No",   0),  # set other_covid_infection_test and other_covid_infection_test_results to Null as the flag passed and dataset is 0
                (None,  "No",   "Any tests negative, but none positive", 1,      "No",   "No",   "No",   1),  # do nothing as dataset is not 0
            ]

        ,
            [
                (None,  None,  None,       0,      None,   None,  "No",    0),  # set other_covid_infection_test and other_covid_infection_test_results to Null as the flag passed and dataset is 0
                (None,  "Yes", "Any tests negative, but none positive", 1,      "No",   "No",  "Yes",   1),  # do nothing as dataset is not 0
            ]
        )
    )
)
def test_clean_inconsistent_event_detail_part_1(spark_session, input_data, expected_data):
    input_df = spark_session.createDataFrame(
        data=input_data,
        schema="think_had_covid_onset_date string, other_covid_infection_test string, other_covid_infection_test_results string, think_had_covid_symptom_count integer, think_had_covid_contacted_nhs string, think_had_covid_admitted_to_hospital string, think_had_covid string, survey_response_dataset_major_version integer",
    )
    expected_df = spark_session.createDataFrame(
        data=expected_data,
        schema="think_had_covid_onset_date string, other_covid_infection_test string, other_covid_infection_test_results string, think_had_covid_symptom_count integer, think_had_covid_contacted_nhs string, think_had_covid_admitted_to_hospital string, think_had_covid string,  survey_response_dataset_major_version integer",
    )
    output_df = clean_inconsistent_event_detail_part_1(input_df)
    assert_df_equality(output_df, expected_df, ignore_nullable=True, ignore_row_order=True, ignore_column_order=True)
