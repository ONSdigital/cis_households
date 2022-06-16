from chispa import assert_df_equality

from cishouseholds.weights.design_weights import calculate_scenario_c_antibody_design_weights


def test_calculate_scenario_c_antibody_design_weights(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            ("new", "Yes", 4, 1.0, 1.0, 4.0, 1.0),
            ("new", "No", 3, 2.0, 2.0, 4.0, 2.0),
            ("previous", "Yes", 2, 1.0, 1.0, 4.0, 4.0),
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
    output_df = calculate_scenario_c_antibody_design_weights(
        df=expected_df.drop("c_weight"),
        column_name_to_assign="c_weight",
        sample_new_previous_column="sample",
        tranche_eligible_column="eligible",
        tranche_number_column="tranche_num",
        swab_design_weight_column="weight",
        tranche_factor_column="factor",
        previous_design_weight_column="hh_weight",
    )
    assert_df_equality(output_df, expected_df, ignore_column_order=True, ignore_row_order=True, ignore_nullable=True)
