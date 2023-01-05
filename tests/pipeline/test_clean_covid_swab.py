from chispa import assert_df_equality

from cishouseholds.pipeline.high_level_transformations import clean_covid_test_swab


def test_clean_covid_test_swab(spark_session):
    #     replace other_covid_infection_test=1 if ==(0|NULL) & other_covid_infection_test_result=1 & (sympt_covid_count>0 | think_had_covid_onset_date!= NULL)
    # replace other_covid_infection_test_result=NULL if ==1 & think_had_covid_onset_date=NULL & sympt_covid_count==0 & think_had_covid_contacted_nhs==0 & think_had_covid_admitted_to_hopsital==0 & other_covid_infection_test==(0|NULL) & think_had_covid==(0|NULL)
    # replace other_covid_infection_test_result=NULL if ==1 & think_had_covid_onset_date=NULL & sympt_covid_count==0 & think_had_covid_contacted_nhs==0 & think_had_covid_admitted_to_hopsital==0 & other_covid_infection_test==0

    # fmt: off
    input_df = spark_session.createDataFrame(
        data=[
            ("A", 0, 1, 0, 0, 0, 0),  # set other_covid_infection_test to 1 as other_covid_infection_test_result == 1
            ("B", None, None, None, None, None, None),  # do nothing
            ("C", None, 1,    None, None, None, None),  # set other_covid_infection_test to 1 as other_covid_infection_test_result == 1
            ("D", 0, 1, 0, 0, 0, 0),  # set other_covid_infection_test to 1 as other_covid_infection_test_result == 1
            ("E", 0, 1, 1, 1, 1, 0),  # set other_covid_infection_test to 1 as think_had_covid_contacted_nhs ==1 and think_had_covid_admitted_to_hopsital == 1
            ("F", 0, 0, 0, 0, 0, 0),  # set other_covid_infection_test_result to Null as all values are 0
            ("G", 0, 0, 0, 0, 1, 1),  # other_covid_infection_test_result is still set to Null when think_had_covid_admitted_to_hopsital and think_had_covid are 1
            (None,0, 1, 0, 0, 1, 0),  # do nothing as other_covid_infection_test_result is not 1 and only 1 of think_had_covid_contacted_nhs and think_had_covid_admitted_to_hopsital is 1
            (None,0, 1, 0, 0, 0, 0),  # set other_covid_infection_test_result to Null as there is no think_had_covid_onset_date
        ],
        schema="think_had_covid_onset_date string, other_covid_infection_test integer, other_covid_infection_test_result integer, sympt_covid_count integer, think_had_covid_contacted_nhs integer, think_had_covid_admitted_to_hopsital integer, think_had_covid integer",
    )
    expected_df = spark_session.createDataFrame(
        data=[
            ("A", 1, 1, 0, 0, 0, 0),
            ("B", None, None, None, None, None, None),
            ("C", 1, 1, None, None, None, None),
            ("D", 1, 1, 0, 0, 0, 0),
            ("E", 1, 1, 1, 1, 1, 0),
            ("F", 0, None, 0, 0, 0, 0),
            ("G", 0, None, 0, 0, 1, 1),
            (None, 0, 1, 0, 0, 1, 0),
            (None, 0, None, 0, 0, 0, 0),
        ],
        schema="think_had_covid_onset_date string, other_covid_infection_test integer, other_covid_infection_test_result integer, sympt_covid_count integer, think_had_covid_contacted_nhs integer, think_had_covid_admitted_to_hopsital integer, think_had_covid integer",
    )
    output_df = clean_covid_test_swab(input_df)
    assert_df_equality(output_df, expected_df, ignore_nullable=True, ignore_row_order=True, ignore_column_order=True)

    # replace other_covid_infection_test=1 if ==(0|NULL) & other_covid_infection_test_result==0 & (sympt_covid_count>0 | think_had_covid_onset_date!=NULL)
    # replace other_covid_infection_test=1 if ==(0|NULL) & other_covid_infection_test_result==0 & think_had_covid_admitted_to_hopsital==1 & think_had_covid_contacted_nhs==1
    # replace other_covid_infection_test_result=NULL if ==0 & other_covid_infection_test==(0|NULL) & think_had_covid_onset_date=NULL & sympt_covid_count==0 & think_had_covid_contacted_nhs==(0|NULL) & think_had_covid_admitted_to_hopsital==(0|NULL) & think_had_covid==(0|NULL)
    # replace other_covid_infection_test_result=. if ==0 & other_covid_infection_test==(0|.) & think_had_covid_onset_date>=. & sympt_covid_count==0 & think_had_covid_contacted_nhs==(0|1) & think_had_covid_admitted_to_hopsital==(0|.) & think_had_covid==0
    # replace other_covid_infection_test_result=NULL if ==0 & other_covid_infection_test==(0|NULL) & think_had_covid_onset_date=NULL & sympt_covid_count==0 & think_had_covid_contacted_nhs==(0|NULL) & think_had_covid_admitted_to_hopsital==(0|1) & think_had_covid=(0|1)
