import pandas as pd
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
from cishouseholds.weights.pre_calibration import survey_extraction_household_data_response_factor

# from cishouseholds.weights.pre_calibration import precalibration_checkpoints


def test_precal_end_to_end(spark_session):

    csv_file_path = "tests/weights/test_files/test_data_dweights_calibration.csv"
    schema = """
        ons_household_id integer,
        participant_id integer,
        sex string,
        ethnicity_white integer,
        age_at_visit integer,
        index_multiple_deprivation integer,
        sample_addressbase_indicator integer,
        country_name_12 string,
        region_code string,
        cis_area_code_20 integer,
        household_level_designweight_swab double,
        household_level_designweight_antibodies double,
        check_if_missing integer,

        swab integer,
        antibodies integer,
        longcovid integer,
        ever_never integer,
        14_days integer,
        28_days integer,
        42_days integer,

        number_of_households_population_by_cis integer,
        response_indicator integer,
        interim_participant_id integer,
        index_multiple_deprivation_group integer,
        population_country_swab integer,
        population_country_antibodies integer,

        total_sampled_households_cis_imd_addressbase integer,
        total_responded_households_cis_imd_addressbase integer,
        raw_non_response_factor double,
        mean_raw_non_response_factor double,
        scaled_non_response_factor double,
        bounded_non_response_factor double,
        household_level_designweight_adjusted_swab double,

        household_level_designweight_adjusted_antibodies double,
        sum_adjusted_design_weight_swab double,
        scaling_factor_adjusted_design_weight_swab double,
        scaled_design_weight_adjusted_swab double,
        sum_adjusted_design_weight_antibodies double,
        scaling_factor_adjusted_design_weight_antibodies double,
        scaled_design_weight_adjusted_antibodies double,

        interim_region_code integer,
        interim_sex integer,
        age_group_swab integer,
        age_group_antibodies integer,

        p1_swab_longcovid_england integer,
        p1_swab_longcovid_wales_scot_ni integer,
        p1_for_antibodies_evernever_engl integer,
        p1_for_antibodies_28daysto_engl integer,
        p1_for_antibodies_wales_scot_ni integer,
        p2_for_antibodies integer,
        p3_for_antibodies_28daysto_engl integer,

        antibodies_28daysto integer,
        antibodies_evernever integer,
        longcovid_24days integer,
        longcovid_42days integer,
        swab_14days integer,
        swab_evernever integer
    """

    df_expected = spark_session.read.csv(
        csv_file_path,
        header=True,
        schema=schema,
        ignoreLeadingWhiteSpace=True,
        ignoreTrailingWhiteSpace=True,
        sep=",",
    )

    df_input_country = spark_session.createDataFrame(
        data=[
            # fmt: off
                ('england',				1500,			1200),
                ('wales',				900,			800),
                ('scotland',			1000,			700),
                ('northern_ireland',	600,			700),
            # fmt: on
        ],
        schema="""
                country_name_12 string,
                population_country_swab integer,
                population_country_antibodies integer
        """,
    )

    df_input = df_expected.drop(
        "response_indicator",
        "multiple_deprivation_group",
        "population_country_swab",
        "population_country_antibodies",
        "total_sampled_households_cis_imd_addressbase",
        "total_responded_households_cis_imd_addressbase",
        "raw_non_response_factor",
        "mean_raw_non_response_factor",
        "scaled_non_response_factor",
        "bounded_non_response_factor",
        "household_level_designweight_adjusted_swab",
        "household_level_designweight_adjusted_antibodies",
        "check_if_missing",
        "sum_adjusted_design_weight_swab",
        "scaling_factor_adjusted_design_weight_swab",
        "scaled_design_weight_adjusted_swab",
        "sum_adjusted_design_weight_antibodies",
        "scaling_factor_adjusted_design_weight_antibodies",
        "scaled_design_weight_adjusted_antibodies",
        "interim_region_code",
        "interim_sex",
        "age_group_swab",
        "age_group_antibodies",
        "p1_swab_longcovid_england",
        "p1_swab_longcovid_wales_scot_ni",
        "p1_for_antibodies_evernever_engl",
        "p1_for_antibodies_28daysto_engl",
        "p1_for_antibodies_wales_scot_ni",
        "p2_for_antibodies",
        "p3_for_antibodies_28daysto_engl",
        "antibodies_28daysto",
        "antibodies_evernever",
        "longcovid_24days",
        "longcovid_42days",
        "swab_14days",
        "swab_evernever",
    )

    df = survey_extraction_household_data_response_factor(
        df=df_input,
        df_extract_by_country=df_input_country,
        required_extracts_column_list=["ons_household_id", "participant_id", "sex", "ethnicity_white", "age_at_visit"],
    )
    df = derive_index_multiple_deprivation_group(df)
    df = derive_total_responded_and_sampled_households(df)
    df = calculate_non_response_factors(df, n_decimals=3)
    df = adjust_design_weight_by_non_response_factor(df)
    df = adjusted_design_weights_to_population_totals(df)

    # TODO: add for loop with test_type
    # check_1, check_2_3, check_4 = precalibration_checkpoints(
    #     df,
    #     test_type="swab",
    #     dweight_list=["household_level_designweight_swab", "household_level_designweight_antibodies"],
    # )

    df = grouping_from_lookup(df)
    df = create_calibration_var(df)

    # df1 = generate_datasets_to_be_weighted_for_calibration(df=df, processing_step=1)
    assert_df_equality(df, df_expected, ignore_column_order=True, ignore_row_order=True, ignore_nullable=True)
