from chispa import assert_df_equality

from cishouseholds.filter import filter_invalid_vaccines


def test_filter_invalid_vaccines(spark_session):
    schema = """
            num_doses string,
            participant_id integer,
            visit_datetime string,
            vaccine_datetime string
        """
    input_df = spark_session.createDataFrame(
        data=[
            # fmt: off
            (1,     1,      "2022-01-01", "2021-01-04"),
            (3,     1,      "2022-01-02", "2021-01-04"), # removed as comes before 2 and over 331 days between dates
            (2,     1,      "2022-01-03", "2021-01-04"),
            (1,     2,      "2022-01-01", "2021-11-04"),
            (3,     2,      "2022-01-02", "2021-11-04"), # not removed as comes before 2 but less than 331 days between dates
            (2,     2,      "2022-01-03", "2021-11-04"),
            (1,     3,      "2022-01-01", "2021-01-04"),
            (3,     3,      "2022-01-02", "2021-01-04"), # removed as comes before 2 and over 331 days between dates
            (5,     3,      "2022-01-01", "2021-01-04"), #removed as comes before 2 and over 331 days between dates
            (2,     3,      "2022-01-03", "2021-01-04"),
            # fmt: on
        ],
        schema=schema,
    )

    expected_df = spark_session.createDataFrame(
        data=[
            # fmt: off
            (1, 1, "2022-01-01", "2021-01-04"),
            (2, 1, "2022-01-03", "2021-01-04"),
            (1, 2, "2022-01-01", "2021-11-04"),
            (3, 2, "2022-01-02", "2021-11-04"),
            (2, 2, "2022-01-03", "2021-11-04"),
            (1, 3, "2022-01-01", "2021-01-04"),
            (2, 3, "2022-01-03", "2021-01-04"),
        ],
        schema=schema,
    )

    output_df = filter_invalid_vaccines(
        df=input_df,
        participant_id_column="participant_id",
        visit_datetime_column="visit_datetime",
        num_doses_column="num_doses",
        vaccine_datetime_column="vaccine_datetime",
    )
    assert_df_equality(expected_df, output_df, ignore_nullable=True, ignore_column_order=True, ignore_row_order=True)
