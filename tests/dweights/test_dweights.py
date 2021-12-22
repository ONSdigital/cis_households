from chispa import assert_df_equality
from pyspark.sql import functions as F

from cishouseholds.weights.pre_calibration import adjust_design_weight_by_non_response_factor
from cishouseholds.weights.pre_calibration import adjusted_design_weights_to_population_totals
from cishouseholds.weights.pre_calibration import calculate_non_response_factors
from cishouseholds.weights.pre_calibration import create_calibration_var
from cishouseholds.weights.pre_calibration import derive_index_multiple_deprivation_group
from cishouseholds.weights.pre_calibration import derive_total_responded_and_sampled_households
from cishouseholds.weights.pre_calibration import generate_datasets_to_be_weighted_for_calibration
from cishouseholds.weights.pre_calibration import grouping_from_lookup
from cishouseholds.weights.pre_calibration import precalibration_checkpoints



def test_derive_index_multiple_deprivation_group(spark_session):
    schema_expected = """country_name_12 string,
                        index_multiple_deprivation integer,
                        index_multiple_deprivation_group integer"""
    data_expected_df = [
        # fmt: off
        ("England", 6569,   1),
        ("engLAND", 15607,  3),
        ("england", 57579,  5),
        ("WALES",   383,    2),
        ("wales",   1042,   3),
        ("Wales",   1358,   4),
        ("Northern Ireland",   160,    1),
        ("NORTHERN ireland",   296,    2),
        ("northern ireland",   823,   5),
        # fmt: on
    ]
    df_expected = spark_session.createDataFrame(data_expected_df, schema=schema_expected)
    df_input = df_expected.drop("index_multiple_deprivation_group")

    df_output = derive_index_multiple_deprivation_group(df_input)

    assert_df_equality(df_output, df_expected, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)


def test_derive_total_responded_and_sampled_households(spark_session):

    schema_expected_df = """ons_household_id string,
                            sample_addressbase_indicator integer,
                            country_name_12 string,
                            cis_area_code_20 integer,
                            index_multiple_deprivation_group integer,
                            interim_participant_id integer,
                            total_sampled_households_cis_imd_addressbase integer,
                            total_responded_households_cis_imd_addressbase integer"""
    data_expected_df = [
        # fmt: off
            ("A1", 1, "england",            1, 1, 1, 5, 3),
            ("A2", 1, "england",            1, 1, 1, 5, 3),
            ("A3", 1, "england",            1, 1, 1, 5, 3),
            ("A4", 1, "england",            1, 1, 0, 5, 3),
            ("A5", 1, "england",            1, 1, 0, 5, 3),
            ("B1", 2, "NORTHERN IRELAND",   2, 2, 1, 4, 2),
            ("B2", 2, "Northern Ireland",   2, 2, 1, 4, 2),
            ("B3", 2, "NORThern irelAND",   2, 2, 0, 4, 2),
            ("B4", 2, "northern ireland",   2, 2, 0, 4, 2),
        # fmt: on
    ]

    df_expected = spark_session.createDataFrame(data_expected_df, schema=schema_expected_df)

    df_input = df_expected.drop(
        "total_sampled_households_cis_imd_addressbase", "total_responded_households_cis_imd_addressbase"
    )
    df_output = derive_total_responded_and_sampled_households(df_input)

    assert_df_equality(df_output, df_expected, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)


def test_calculate_non_response_factors(spark_session):
    schema_expected_df = """country_name_12 string,
                            total_sampled_households_cis_imd_addressbase integer,
                            total_responded_households_cis_imd_addressbase integer,
                            raw_non_response_factor double,
                            mean_raw_non_response_factor double,
                            scaled_non_response_factor double,
                            bounded_non_response_factor double"""
    data_expected_df = [
        # fmt: off
            # scaled_non_response has to be smaller than 0.5 or larger than 2.0 non-inclusive
            ("England", 20, 6, 3.3, 5.0, 0.7, None),
            ("England", 17, 12, 1.4, 5.0, 0.3, 0.6),
            ("England", 25, 19, 1.3, 5.0, 0.3, 0.6),
            ("England", 28, 2, 14.0, 5.0, 2.8, 1.8),
            ("Northern Ireland", 15, 10, 1.5, 3.2, 0.5, None),
            ("Northern Ireland", 11, 2, 5.5, 3.2, 1.7, None),
            ("Northern Ireland", 9, 7, 1.3, 3.2, 0.4, 0.6),
            ("Northern Ireland", 18, 4, 4.5, 3.2, 1.4, None),
        # fmt: off
    ]

    df_expected = spark_session.createDataFrame(data_expected_df, schema=schema_expected_df)

    df_input = df_expected.drop("raw_non_response_factor")

    df_output = calculate_non_response_factors(
        df=df_input,
        n_decimals=1
    )
    assert_df_equality(df_output, df_expected, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)


def test_adjust_design_weight_by_non_response_factor(spark_session):
    schema_expected = """response_indicator integer,
                        household_level_designweight_swab double,
                        household_level_designweight_antibodies double,
                        bounded_non_response_factor double,
                        household_level_designweight_adjusted_swab double,
                        household_level_designweight_adjusted_antibodies double"""
    data_expected_df = [
        (1, 1.3, 1.6, 1.8, 2.3, 2.9),
        (1, 1.7, 1.4, 0.6, 1.0, 0.8),
        (0, 1.3, 1.6, 1.8, None, None),
    ]

    df_expected = spark_session.createDataFrame(data_expected_df, schema=schema_expected)

    df_input = df_expected.drop(
        "household_level_designweight_adjusted_swab", "household_level_designweight_adjusted_antibodies"
    )

    df_output = adjust_design_weight_by_non_response_factor(df_input)

    assert_df_equality(df_output, df_expected, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)


def test_adjusted_design_weights_to_population_totals(spark_session):
    schema_expected = """country_name_12 string,
                         response_indicator integer,
                         household_level_designweight_adjusted_swab double,
                         household_level_designweight_adjusted_antibodies double,
                         population_country_swab integer,
                         population_country_antibodies integer,
                         sum_adjusted_design_weight_swab double,
                         sum_adjusted_design_weight_antibodies double,
                         scaling_factor_adjusted_design_weight_swab double,
                         scaling_factor_adjusted_design_weight_antibodies double,
                         scaled_design_weight_adjusted_swab double,
                         scaled_design_weight_adjusted_antibodies double
                         """
    data_expected_df = [
        ("England", 1, 1.2, 1.4, 250, 350, 5.8, 5.4, 43.1, 64.8, 51.7, 90.7),
        ("England", 1, 1.5, 1.2, 250, 350, 5.8, 5.4, 43.1, 64.8, 64.7, 77.8),
        ("England", 1, 1.8, 1.6, 250, 350, 5.8, 5.4, 43.1, 64.8, 77.6, 103.7),
        ("England", 1, 0.7, 0.4, 250, 350, 5.8, 5.4, 43.1, 64.8, 30.2, 25.9),
        ("England", 1, 0.6, 0.8, 250, 350, 5.8, 5.4, 43.1, 64.8, 25.9, 51.8),
        ("England", 0, None, None, 250, 350, None, None, None, None, None, None),
        ("England", 0, None, None, 250, 350, None, None, None, None, None, None),
    ]

    df_expected = spark_session.createDataFrame(data_expected_df, schema=schema_expected)

    df_input = df_expected.drop("sum_adjusted_design_weight_swab double", "sum_adjusted_design_weight_antibody double")

    df_output = adjusted_design_weights_to_population_totals(df_input)

    assert_df_equality(df_output, df_expected, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)


def test_precalibration_checkpoints(spark_session):
    schema = """
                number_of_households_population_by_cis double,
                scaled_design_weight_adjusted_swab double,
                dweight_1 double,
                dweight_2 double,
                not_positive_or_null integer
            """
    expected_df_not_pass = spark_session.createDataFrame(
        data=[
            # fmt: off
                (2.0,     1.0,   2.5,    1.0,    1),
                (2.0,     1.0,   -1.5,   1.2,    None),
                (2.0,     1.0,   -1.5,   None,   None),
            # fmt: on
        ],
        schema=schema,
    )

    input_df_not_pass = expected_df_not_pass.drop("not_positive_or_null")

    check_1, check_2_3, check_4 = precalibration_checkpoints(
        df=input_df_not_pass,
        test_type="swab",
        dweight_list=["dweight_1", "dweight_2"],
    )
    assert check_1 is not True
    assert check_2_3 is not True
    assert check_4 is True


    expected_df_pass = spark_session.createDataFrame(
        data=[
            # fmt: off
                (3.0,     1.0,   2.5,   1.0,   None),
                (3.0,     1.0,   1.5,   1.2,   None),
                (3.0,     1.0,   1.5,   1.7,   None),
            # fmt: on
        ],
        schema=schema,
    )

    input_df_pass = expected_df_pass.drop("not_positive_or_null")

    check_1, check_2_3, check_4 = precalibration_checkpoints(
        df=input_df_pass,
        test_type="swab",
        dweight_list=["dweight_1", "dweight_2"],
    )
    assert check_1 is True
    assert check_2_3 is True
    assert check_4 is True


def test_grouping_from_lookup(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            # fmt: off
                ("E12000001", 1,    'male',     1,      2,  1,  None), # for swabs
                ("E12000007", 7,    'female',   2,      25, 4,  2),
                ("N99999999", 12,   None,       None,   70, 7,  5),
            # fmt: on
        ],
        schema="""
                region_code string,
                interim_region_code integer,
                sex string,
                interim_sex integer,
                age_at_visit integer,
                age_group_swab integer,
                age_group_antibodies integer
            """,
    )
    input_df = expected_df.drop("interim_region_code", "interim_sex", "age_group_swab", "age_group_antibodies")

    output_df = grouping_from_lookup(df=input_df)

    assert_df_equality(output_df, expected_df, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)


def test_create_calibration_var(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            # fmt: off
                ('england',			    1,1,1,16, 3, 2,1,	1,		1, 1, 1,1, 1,1,		    3, 		None, 	2,		2, 		None,	    2,		1, 		1, 1, 1, 1, 1, 1),
                ('wales',				1,1,1,16, 3, 2,1, 	None,	1, 1, 1,1, 1,1, 		None,	3,		None, 	None,	None,		None, 	None, 	1, None, 1, 1, None, None),
                ('northern_ireland',	2,1,1,16, 3, 2,1, 	None,	1, 1, 1,1, 1,1, 		None,	3,		None, 	None,	None, 		None, 	None, 	1, None, 1, 1, None, None),
                ('scotland',			3,1,1,16, 3, 2,1, 	None,	1, 1, 1,1, 1,1, 		None,	3,		None, 	None,	None, 		None, 	None, 	1, None, 1, 1, None, None),
            # fmt: on
        ],
        schema="""
                country_name_12 string,
                interim_region_code integer,
                interim_sex integer,
                ethnicity_white integer,
                age_at_visit integer,
                age_group_swab integer,
                age_group_antibodies integer,

                swab integer,
                antibodies integer,
                ever_never integer,
                longcovid integer,
                14_days integer,
                28_days integer,
                24_days integer,
                42_days integer,

                p1_swab_longcovid_england integer,
                p1_swab_longcovid_wales_scot_ni integer,
                p1_for_antibodies_evernever_engl integer,
                p1_for_antibodies_28daysto_engl integer,
                p1_for_antibodies_wales_scot_ni integer,
                p2_for_antibodies integer,
                p3_for_antibodies_28daysto_engl integer,

                swab_evernever integer,
                swab_14days integer,
                longcovid_24days integer,
                longcovid_42days integer,
                antibodies_evernever integer,
                antibodies_28daysto integer
            """,
    )

    input_df = expected_df.drop(
        'p1_swab_longcovid_england',
        'p1_swab_longcovid_wales_scot_ni',
        'p1_for_antibodies_evernever_engl',
        'p1_for_antibodies_28daysto_engl',
        'p1_for_antibodies_wales_scot_ni',
        'p2_for_antibodies',
        'p3_for_antibodies_28daysto_engl',
        'swab_evernever',
        'swab_14days',
        'longcovid_24days',
        'longcovid_42days',
        'antibodies_evernever',
        'antibodies_28daysto',
    )
    output_df = create_calibration_var(df=input_df)
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
        schema="""country_name_12 string,
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
        schema="""country_name_12 string,
                participant_id integer,
                scaled_design_weight_adjusted_antibodies double,
                p1_for_antibodies_wales_scot_ni double""",
    )
    output_df = generate_datasets_to_be_weighted_for_calibration(df=input_df, processing_step=6)

    assert_df_equality(output_df, expected_df, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)
