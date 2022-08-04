from chispa import assert_df_equality

from cishouseholds.weights.derive import assign_tranche_factor


def test_assign_tranche_factor(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            ("A", "S1", 1, "Yes", None),
            ("B", "S1", 1, "Yes", None),
            ("C", "S1", 2, "Yes", 2.0),  # 4 eligible / 2 max tranche
            ("D", "S1", 2, "Yes", 2.0),  # 4 eligible / 2 max tranche
            ("D", "S2", 2, "Yes", 1.0),  # 1 elibgible / 1 max tranche
            (None, "S2", None, "No", None),
        ],
        schema="""
            id string,
            stratum string,
            tranche_number_indicator integer,
            tranche_eligible_households string,
            tranche_factor double""",
    )
    output_df = assign_tranche_factor(
        df=expected_df.drop("tranche_factor"),
        column_name_to_assign="tranche_factor",
        tranche_column="tranche_number_indicator",
        elibility_column="tranche_eligible_households",
        strata_columns=["stratum"],
        household_id_column="id",
    )
    assert_df_equality(
        output_df,
        expected_df,
        ignore_nullable=True,
        ignore_row_order=True,
        ignore_column_order=True,
    )
