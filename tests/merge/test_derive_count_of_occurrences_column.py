from chispa import assert_df_equality

from cishouseholds.merge import assign_count_of_occurrences_column


def test_derive_count_of_occurrences_column(spark_session):

    expected_df = spark_session.createDataFrame(
        data=[
            ("1", 1),
            ("2", 2),
            ("2", 2),
            ("3", 1),
            ("4", 3),
            ("4", 3),
            ("4", 3),
        ],
        schema="id string, count_id integer",
    )

    input_df = expected_df.drop("count_id")

    output_df = assign_count_of_occurrences_column(input_df, "id", "count_id")

    assert_df_equality(output_df.orderBy("id"), expected_df, ignore_nullable=True)
