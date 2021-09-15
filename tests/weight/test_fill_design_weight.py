from chispa import assert_df_equality

from cishouseholds.weight import fill_design_weight


def test_fill_design_weight(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[(1.0, 0.0, 1.0), (None, 1.0, 1.0), (None, None, None), (None, None, 1.0)],
        schema="""
        previous_antibody_design_weight double,
        swab_design_weight double,
        new_antibody_design_weight double
        """,
    )

    input_df = expected_df.drop("new_antibody_design_weight")

    output_df = fill_design_weight(
        input_df,
        column_name_to_assign="new_antibody_design_weight",
        previous_antibody_design_weight_column="previous_antibody_design_weight",
        swab_design_weight_column="swab_design_weight",
    )

    assert_df_equality(output_df, expected_df, ignore_nullable=True)
