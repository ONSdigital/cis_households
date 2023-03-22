import pyspark.sql.functions as F
import pytest
from chispa import assert_df_equality

from cishouseholds.phm.reporting_class import Report


@pytest.fixture
def input_df(spark_session):
    schema = """
            id integer,
            start string,
            end string,
            visit_date string,
            status string
        """

    input_df = spark_session.createDataFrame(
        # fmt: off
        data=[
            # window 1
            # - day 1
            (1, "2021-01-01", "2021-01-11", "2021-01-01", "Completed"),
            (2, "2021-01-01", "2021-01-11", "2021-01-01", "Completed"),
            # - day 2
            (3, "2021-01-01", "2021-01-11", "2021-01-02", "Completed"),
            (4, "2021-01-01", "2021-01-11", "2021-01-02", "Partially Completed"),

            # window 2
            # - day 1
            (4, "2021-02-02", "2021-02-12", "2021-02-02", "Completed"),
            # - day 2
            (5, "2021-02-02", "2021-02-12", "2021-02-03", "Completed"),
            # - day 3
            (6, "2021-02-02", "2021-02-12", "2021-02-04", "Partially Completed"),
            # - day 4
            (7, "2021-02-02", "2021-02-12", "2021-02-05", "Partially Completed"),
            (8, "2021-02-02", "2021-02-12", "2021-02-05", "Completed"),
            (9, "2021-02-02", "2021-02-12", "2021-02-05", "Partially Completed"),

            # window 3
            # - day 9
            (10, "2022-01-02", "2022-01-13", "2022-01-11", "Completed"), # all participants regardless of amount Partially Completed on same day so 100%
            (11, "2022-01-02", "2022-01-13", "2022-01-11", "Completed"),

            # window 4
            # - day 10
            (12, "2022-01-02", "2022-01-12", "2022-01-12", "Completed"),
        ],
        # fmt: on
        schema=schema,
    )
    for col in ["start", "end", "visit_date"]:
        input_df = input_df.withColumn(col, F.to_timestamp(col))
    return input_df


def test_create_completion_table_month(spark_session, input_df):
    schema = """
            day_1 double,
            day_2 double,
            day_3 double,
            day_4 double,
            day_5 double,
            day_6 double,
            day_7 double,
            day_8 double,
            day_9 double,
            day_10 double,
            day_11 double,
            day_12 double,
            day_13 double,
            day_14 double,
            day_15 double,
            total double,
            date_range string
        """

    expected_partial_df = spark_session.createDataFrame(
        # fmt: off
        data=[
            (0.0, 0.25, 0.0, 0.0, 0.0,                 0.0,                0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.25, "2021-01-01-2021-01-15"),
            (0.0, 0.0,  0.0, 0.0, 0.16666666666666666, 0.3333333333333333, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.5, "2021-01-31-2021-02-14"),
            (0.0, 0.0,  0.0, 0.0, 0.0,                 0.0,                0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, "2022-01-11-2022-01-25",),
        ],
        # fmt: on
        schema=schema
    )

    expected_full_df = spark_session.createDataFrame(
        # fmt: off
        data=[
            (0.5,                 0.25,                0.0,                 0.0,                 0.0, 0.0,                 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.75, "2021-01-01-2021-01-15"),
            (0.0,                 0.0,                 0.16666666666666666, 0.16666666666666666, 0.0, 0.16666666666666666, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.5, "2021-01-31-2021-02-14"),
            (0.66666666666666666, 0.3333333333333333,  0.0,                 0.0,                 0.0, 0.0,                 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, "2022-01-11-2022-01-25"),
        ],
        # fmt: on
        schema=schema
    )
    report = Report()
    partial_df, full_df = report.create_completion_table_set_range(
        df=input_df,
        participant_id_column="id",
        window_start_column="start",
        window_end_column="end",
        window_status_column="status",
        reference_date_column="visit_date",
        window_range=15,
    )

    assert_df_equality(
        partial_df, expected_partial_df, ignore_nullable=True, ignore_row_order=True, ignore_column_order=True
    )
    assert_df_equality(full_df, expected_full_df, ignore_nullable=True, ignore_row_order=True, ignore_column_order=True)


def test_create_completion_table_day(spark_session, input_df):
    schema = """
        start string,
        end string,
        day_1 double,
        day_2 double,
        day_3 double,
        day_4 double,
        day_5 double,
        day_6 double,
        day_7 double,
        day_8 double,
        day_9 double,
        day_10 double,
        day_11 double,
        total double
    """

    expected_partial_df = spark_session.createDataFrame(
        # fmt: off
        data=[
            ("2021-01-01", "2021-01-11", 0.0, 0.25, 0.0,                 0.0,                0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.25),
            ("2021-02-02", "2021-02-12", 0.0, 0.0,  0.16666666666666666, 0.3333333333333333, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.5),
            ("2022-01-02", "2022-01-12", 0.0, 0.0,  0.0,                 0.0,                0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
            ("2022-01-02", "2022-01-13", 0.0, 0.0,  0.0,                 0.0,                0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
        ],
        # fmt: on
        schema=schema
    )

    expected_full_df = spark_session.createDataFrame(
        # fmt: off
        data=[
            ("2021-01-01", "2021-01-11", 0.5,                 0.25,                0.0, 0.0,                 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.75),
            ("2021-02-02", "2021-02-12", 0.16666666666666666, 0.16666666666666666, 0.0, 0.16666666666666666, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.5),
            ("2022-01-02", "2022-01-12", 0.0,                 0.0,                 0.0, 0.0,                 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0),
            ("2022-01-02", "2022-01-13", 0.0,                 0.0,                 0.0, 0.0,                 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0),
        ],
        # fmt: on
        schema=schema
    )

    for col in ["start", "end"]:
        expected_full_df = expected_full_df.withColumn(col, F.to_timestamp(col))
        expected_partial_df = expected_partial_df.withColumn(col, F.to_timestamp(col))

    report = Report()
    partial_df, full_df = report.create_completion_table_days(
        df=input_df,
        participant_id_column="id",
        window_start_column="start",
        window_end_column="end",
        window_status_column="status",
        reference_date_column="visit_date",
    )

    assert_df_equality(
        partial_df, expected_partial_df, ignore_nullable=True, ignore_row_order=True, ignore_column_order=True
    )
    assert_df_equality(full_df, expected_full_df, ignore_nullable=True, ignore_row_order=True, ignore_column_order=True)
