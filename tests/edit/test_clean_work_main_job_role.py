from chispa import assert_df_equality

from cishouseholds.edit import clean_job_description_string


def test_clean_work_main_job_role(spark_session):

    input_df = spark_session.createDataFrame(
        data=[
            (1, "good&MORning  "),
            (2, "HELLO-ther    e vargass"),
            (3, " WELL WELL-well "),
            (4, " NA "),
        ],
        schema="id integer, col1 string",
    )

    expected_df = spark_session.createDataFrame(
        data=[
            (1, "GOOD&MORNING"),
            (2, "HELLO THER E VARGASS"),
            (3, "WELL WELL WELL"),
            (4, None),
        ],
        schema="id integer, col1 string",
    )

    output_df = clean_job_description_string(input_df, "col1")

    assert_df_equality(output_df, expected_df, ignore_row_order=True, ignore_column_order=True)
