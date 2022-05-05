from chispa import assert_df_equality

from cishouseholds.derive import assign_outward_postcode


def test_assign_outer_postcode(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[("E26 4LB", "E26"), ("E6 2PC", "E6"), ("IG120DB", "IG12")],
        schema=["ref", "outer_postcode"],
    )
    output_df = assign_outward_postcode(expected_df.drop("outer_postcode"), "outer_postcode", "ref")
    assert_df_equality(expected_df, output_df)
