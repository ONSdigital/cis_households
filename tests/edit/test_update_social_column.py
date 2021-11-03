from chispa import assert_df_equality

from cishouseholds.edit import update_social_column


def test_update_social_column(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[("health", "health"), (None, None), ("social", "health")],
        schema="social string, health string",
    )
    input_df = spark_session.createDataFrame(
        data=[
            (None, "health"),
            (None, None),
            ("social", "health"),
        ],
        schema="social string, health string",
    )
    output_df = update_social_column(input_df, "social", "health")
    assert_df_equality(output_df, expected_df, ignore_row_order=True)
