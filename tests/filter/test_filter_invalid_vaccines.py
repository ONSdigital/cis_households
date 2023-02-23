from chispa import assert_df_equality

from cishouseholds.filter import filter_invalid_vaccines


def test_filter_invalid_vaccines(spark_session):  # test funtion
    input_df = spark_session.createDataFrame(
        data=[
            (1, "2021-07-19", "2020-07-19", 1),
            (1, "2021-07-20", "2020-07-20", 3),  # will be filtered as out of order and 330+ day gap
            (1, "2021-07-21", "2020-07-21", 2),
            (2, "2021-07-19", "2020-11-19", 1),
            (2, "2021-07-20", "2020-11-20", 3),  # wont be filtered as not 330 day gap
            (2, "2021-07-21", "2020-11-21", 2),
            (3, "2021-07-19", "2020-11-19", 1),
            (3, "2021-07-20", "2020-11-20", 2),
            (3, "2021-07-21", "2020-11-21", 3),  # wont be filtered as correct order
        ],
        schema="id integer, vaccine_date string, visit_datetime string, num_doses integer",
    )
    expected_df = spark_session.createDataFrame(
        data=[
            (1, "2021-07-19", "2020-07-19", 1),
            (1, "2021-07-21", "2020-07-21", 2),
            (2, "2021-07-19", "2020-11-19", 1),
            (2, "2021-07-20", "2020-11-20", 3),  # wont be filtered as not 330 day gap
            (2, "2021-07-21", "2020-11-21", 2),
            (3, "2021-07-19", "2020-11-19", 1),
            (3, "2021-07-20", "2020-11-20", 2),
            (3, "2021-07-21", "2020-11-21", 3),  # wont be filtered as correct order
        ],
        schema="id integer, vaccine_date string, visit_datetime string, num_doses integer",
    )
    output_df = filter_invalid_vaccines(
        df=input_df,
        vaccine_date_column="vaccine_date",
        visit_datetime_column="visit_datetime",
        num_doses_column="num_doses",
        participant_id_column="id",
    )

    assert_df_equality(output_df, expected_df, ignore_row_order=True, ignore_column_order=True)
