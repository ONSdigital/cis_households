from chispa import assert_df_equality

from cishouseholds.derive import assign_proportion_column


def test_assign_proportion_column(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            (0, "N", 0.5),
            (0, "S", 0.6666666666666666),
            (1, "E", 1.0),
            (1, "N", 0.5),
            (1, "S", 0.6666666666666666),
            (1, "S", 0.6666666666666666),
            (1, "W", 1.0),
        ],
        schema="""white integer, country string, prop double""",
    )
    output_df = assign_proportion_column(expected_df.drop("prop"), "prop", "white", "country", 1)
    assert_df_equality(expected_df, output_df, ignore_row_order=True)
