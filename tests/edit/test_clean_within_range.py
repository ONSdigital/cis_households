from chispa import assert_df_equality

from cishouseholds.edit import clean_within_range


def test_clean_within_range(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            (2, 1),
            (11, 2),
            (None, 3),
        ],
        schema="""ref integer, id integer""",
    )

    expected_df = spark_session.createDataFrame(
        data=[
            (2, 1),
            (None, 2),
            (None, 3),
        ],
        schema="""ref integer, id integer""",
    )

    output_df = clean_within_range(input_df, "ref", [1, 10])
    assert_df_equality(expected_df, output_df, ignore_nullable=True)
