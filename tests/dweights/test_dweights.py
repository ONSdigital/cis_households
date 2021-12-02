from chispa import assert_df_equality

from cishouseholds.dweights_1167 import chose_scenario_of_dweight_for_antibody_different_household
from cishouseholds.dweights_1167 import create_calibration_var
from cishouseholds.dweights_1167 import generate_datasets_to_be_weighted_for_calibration


def test_chose_scenario_of_dweight_for_antibody_different_household(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[],
        schema="",
    )

    input_df = expected_df.drop("")

    output_df = chose_scenario_of_dweight_for_antibody_different_household()

    assert_df_equality(output_df, expected_df, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)


def test_create_calibration_var(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            # fmt: off
                ('england',     1, 1, 1, 3, 2, 		1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0,),
                ('wales',       1, 1, 1, 3, 2, 		1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0,),
                ('england',     1, 1, 1, 3, 2, 		1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0,),
                ('england',     1, 1, 1, 3, 2, 		1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0,),
                ('england',     1, 1, 1, 3, 2, 		1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0,),
                ('england',     1, 1, 1, 3, 2, 		1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0,),
                ('england',     1, 1, 1, 3, 2, 		1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0,),
                ('england',     1, 1, 1, 3, 2, 		1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0,),
            # fmt: on
        ],
        schema="""
                country_name string,
                interim_region_code integer,
                interim_sex integer,
                ethnicity_white integer,
                age_group_swab integer,
                age_group_antibodies integer,

                p1_swab_longcovid_england float,
                p1_swab_longcovid_wales_scot_ni float,
                p1_for_antibodies_evernever_engl float,
                p1_for_antibodies_28daysto_engl float,
                p1_for_antibodies_wales_scot_ni float,
                p2_for_antibodies float,
                p3_for_antibodies_28daysto_engl float
            """,
    )

    input_df = expected_df.drop("")

    output_df = chose_scenario_of_dweight_for_antibody_different_household()

    assert_df_equality(output_df, expected_df, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)


def test_generate_datasets_to_be_weighted_for_calibration(spark_session):

    input_df = spark_session.createDataFrame(
        data=[
            # fmt: off
            ('england',         1, 0.6,  1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0),
            ('england',         2, 0.6,  1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0),
            ('wales',           1, 0.7,  1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0),
            ('scotland',        1, 0.6,  1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0),
            ('northen_ireland', 1, 0.6,  1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0),
            # fmt: on
        ],
        schema="""country_name string,
                participant_id integer,
                scaled_design_weight_adjusted_swab double,
                p1_swab_longcovid_england double,
                p1_swab_longcovid_wales_scot_ni double,
                scaled_design_weight_adjusted_antibodies double,
                p1_for_antibodies_evernever_engl double,
                p2_for_antibodies double,
                p1_for_antibodies_28daysto_engl double,
                p3_for_antibodies double,
                p1_for_antibodies_wales_scot_ni double""",
    )

    expected_df = spark_session.createDataFrame(
        data=[
            # fmt: off
            ('scotland',            1,      1.0,    1.0),
            ('northen_ireland',     1,      1.0,    1.0),
            # fmt: on
        ],
        schema="""country_name string,
                participant_id integer,
                scaled_design_weight_adjusted_antibodies double,
                p1_for_antibodies_wales_scot_ni double""",
    )

    output_df = generate_datasets_to_be_weighted_for_calibration(df=input_df, processing_step=6)

    assert_df_equality(output_df, expected_df, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)
