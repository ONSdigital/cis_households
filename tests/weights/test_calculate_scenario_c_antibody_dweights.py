from chispa import assert_df_equality

from cishouseholds.weights.weights import calculate_scenario_c_antibody_dweights


def test_calculate_scenario_c_antibody_dweights(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            ("new", "Yes", 4, 1.0, 1.0, 4.0, 1.0),
            ("new", "No", 3, 2.0, 2.0, 4.0, 2.0),
            ("previous", "Yes", 2, 1.0, 1.0, 4.0, None),
        ],
        schema="""
            sample string,
            eligible string,
            tranche_num integer,
            weight double,
            factor double,
            hh_weight double,
            c_weight double
            """,
    )
    output_df = calculate_scenario_c_antibody_dweights(
        df=expected_df.drop("c_weight"),
        column_name_to_assign="c_weight",
        sample_new_previous_column="sample",
        tranche_eligible_column="eligible",
        tranche_num_column="tranche_num",
        design_weight_column="weight",
        tranche_factor_column="factor",
        household_dweight_column="hh_weight",
    )
    assert_df_equality(output_df, expected_df, ignore_column_order=True, ignore_row_order=True, ignore_nullable=True)
