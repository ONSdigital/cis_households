from chispa import assert_df_equality

from cishouseholds.weights.weights import carry_forward_design_weights


def test_carry_forward_design_weights(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[(2, 1.0, 4, 1.0, 1.0, 4.0, 1.0), (2, 3.0, 4, 3.0, 1.0, 4.0, 3.0), (1, 1.0, 6, 6.0, 6.0, 1.0, 1.0)],
        schema="""
            groupby integer,
            raw_design_weight_antibodies_ab double,
            num_hh integer,
            scaled_design_weight_antibodies_nonadjusted double,
            scaling_factor_carryforward_design_weight_antibodies double,
            sum_carryforward_design_weight_antibodies double,
            carryforward_design_weight_antibodies double
            """,
    )
    output_df = carry_forward_design_weights(
        df=expected_df.drop(
            "carryforward_design_weight_antibodies",
            "sum_carryforward_design_weight_antibodies",
            "scaling_factor_carryforward_design_weight_antibodies",
            "scaled_design_weight_antibodies_nonadjusted",
        ),
        scenario="A",
        groupby_column="groupby",
        household_population_column="num_hh",
    )
    assert_df_equality(output_df, expected_df, ignore_column_order=True, ignore_row_order=True, ignore_nullable=True)
