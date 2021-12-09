from chispa import assert_df_equality

from cishouseholds.weights.derive import assign_tranche_factor


def test_assign_tranche_factor(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            ("A", "J1", 1, "Yes", 2, 0, "missing"),
            ("B", "J2", 2, "Yes", 2, 2, 1.0),
            ("C", "J2", 2, "Yes", 2, 2, 1.0),
            ("D", "J1", 2, "Yes", 2, 1, 2.0),
            (None, "J1", 1, "No", 0, 0, "missing"),
        ],
        schema="""
            barcode string,
            groupby string,
            tranche integer,
            tranche_eligible_households string,
            number_eligible_households_tranche_bystrata_enrolment long,
            number_sampled_households_tranche_bystrata_enrolment long,
            tranche_factor string""",
    )
    output_df = assign_tranche_factor(
        df=expected_df.drop(
            "tranche_factor",
            "tranche_eligible_households",
            "number_eligible_households_tranche_bystrata_enrolment",
            "number_sampled_households_tranche_bystrata_enrolment",
        ),
        column_name_to_assign="tranche_factor",
        barcode_column="barcode",
        tranche_column="tranche",
        group_by_columns=["groupby"],
    )
    assert_df_equality(output_df, expected_df, ignore_nullable=True, ignore_row_order=True)
