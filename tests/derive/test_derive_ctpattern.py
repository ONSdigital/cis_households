from chispa import assert_df_equality

from cishouseholds.derive import derive_ctpattern


def test_derive_ctpattern(spark_session):
    column_names = ["ct_or", "ct_n", "ct_s"]
    expected_df = spark_session.createDataFrame(
        data=[
            (3, 0, 0, "OR only"),
            (0, 0, 0, None),
            (37, 10, 0, "OR+N"),
            (25, 17, 7, "OR+N+S"),
            (None, None, None, None),
            (None, None, 4, "S only"),
        ],
        schema=column_names + ["ctpattern"],
    )

    input_df = expected_df.drop("ctpattern")

    actual_df = derive_ctpattern(input_df, column_names, spark_session)

    assert_df_equality(actual_df, expected_df)