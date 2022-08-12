from chispa import assert_df_equality

from cishouseholds.weights.derive import assign_tranche_factor


def test_assign_tranche_factor(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            ("A", "S1", 1, "Yes", 2, 1, 2.0, 100.0),
            ("B", "S1", 1, "Yes", 2, None, None, None),
            ("C", "S1", 1, "No", 1, None, None, None),
        ],
        schema="""
            id string,
            stratum string,
            tranche_number_indicator integer,
            tranche_eligible_households string,
            households_by_eligibility_and_strata long,
            sampled_count integer,
            tranche_factor double,
            eligibility_percentage double
            """,
    )
    output_df = assign_tranche_factor(
        df=expected_df.drop("tranche_factor", "eligibility_percentage", "eligible_households_by_strata"),
        tranche_factor_column_name_to_assign="tranche_factor",
        eligibility_percentage_column_name_to_assign="eligibility_percentage",
        sampled_households_count="sampled_count",
        household_id_column="id",
        tranche_column="tranche_number_indicator",
        eligibility_column="tranche_eligible_households",
        strata_columns=["stratum"],
    )
    assert_df_equality(
        output_df,
        expected_df,
        ignore_nullable=True,
        ignore_row_order=True,
        ignore_column_order=True,
    )
