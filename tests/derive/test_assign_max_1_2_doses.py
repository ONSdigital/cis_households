from chispa import assert_df_equality

from cishouseholds.derive import assign_max_1_2_doses


def test_assign_max_1_2_doses(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            (1, "2021-07-19", 1, 1),
            (1, "2021-07-20", 2, 2),
            (1, "2021-07-21", 1, 2),
            (1, "2021-07-22", 3, 2),
        ],
        schema="id integer, vaccine_date string, num_doses integer, max integer",
    )
    output_df = assign_max_1_2_doses(expected_df.drop("max"), "max", "id", "vaccine_date", "num_doses")
    assert_df_equality(output_df, expected_df)
