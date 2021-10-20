from chispa import assert_df_equality

from cishouseholds.derive import assign_test_target


def test_assign_test_target(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[("test_S", "S"), ("gibberish_N", "N"), ("nothing", None)],
        schema="""filename string, target string""",
    )
    output_df = assign_test_target(expected_df.drop("target"), "target", "filename")
    assert_df_equality(expected_df, output_df, ignore_nullable=True)
