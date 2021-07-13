from chispa import assert_df_equality

from cishouseholds.derive import derive_from_list


def test_derive_from_list(spark_session):

    column_names = ["ctpattern", "ctonetarget"]

    expected_df = spark_session.createDataFrame(
        data=[("OR only", 1), ("N only", 1), ("OR+N", 0), ("OR+N+S", 0), (None, None), ("S only", 1)],
        schema=column_names,
    )

    value_list = ["OR only", "N only", "S only"]

    input_df = expected_df.drop("ctonetarget")

    actual_df = derive_from_list(input_df, value_list, "ctonetarget", "ctpattern")

    assert_df_equality(actual_df, expected_df)
