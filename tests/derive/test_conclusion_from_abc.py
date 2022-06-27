from chispa import assert_df_equality

from cishouseholds.derive import conclusion_from_abc


def test_conclusion_from_abc(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            # fmt: off
            ("yes",  "no",    None,    "yes"), # check if some col are yes
            ("yes",  "yes",   "yes",   "yes"), # check if all col are yes
            ("no",   "maybe", None,    "no"), # add a different word from yes/no
            (None,   None,    None,    "no"),
            # test if all col are None
            # fmt: on
        ],
        schema="question_a string, question_b string, question_c string, conclusion string",
    )
    input_df = expected_df.drop("conclusion")

    output_df = conclusion_from_abc(
        df=input_df,
        column_question_list=["question_a", "question_b", "question_c"],
        column_name_to_assign="conclusion",
        unused_input=6,
    )
    assert_df_equality(output_df, expected_df, ignore_column_order=True, ignore_row_order=True, ignore_nullable=True)
