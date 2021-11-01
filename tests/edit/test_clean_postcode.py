from chispa import assert_df_equality

from cishouseholds.edit import clean_postcode


def test_clean_postcode(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            ("     I G $ 4 0   D.B          .   ", 1),
            ("     ., ; wb $ 1 % 6 ofj          )   ", 2),
            (" aaa    ., ; wb $ 1 % 6 ofj          )   ", 3),
        ],
        schema="""ref string, id integer""",
    )

    expected_df = spark_session.createDataFrame(
        data=[
            ("IG4 0DB", 1),
            ("WB16 OFJ", 2),
            (None, 3),
        ],
        schema="""ref string, id integer""",
    )

    output_df = clean_postcode(input_df, "ref")
    assert_df_equality(expected_df, output_df, ignore_nullable=True)
