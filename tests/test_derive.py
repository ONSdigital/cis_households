from cishouseholds.derive import derive_ctpattern


def test_derive_ctpattern(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[(3, 0, 0, "OR only"), (0, 0, 0, None), (37, 10, 0, "OR+N"), (25, 17, 7, "OR+N+S")],
        schema=["ct_or", "ct_n", "ct_s", "ctpattern"],
    )

    input_df = expected_df.drop("ct_pattern")

    actual_df = derive_ctpattern(input_df, spark_session)

    assert actual_df.collect() == expected_df.collect()
