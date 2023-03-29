import pyspark.sql.functions as F
from chispa import assert_df_equality

from cishouseholds.derive import assign_valid_order
from cishouseholds.edit import update_valid_order_2


def test_assign_valid_order(spark_session):
    expected_df = spark_session.createDataFrame(
        # fmt: off
        data=[
            (1, "2021-01-01", "Pfizer",             "2021-01-02 00:00:00.0",        1,      9),
            (1, "2021-01-30", "Pfizer",             "2021-02-02 00:00:00.0",        2,      3),
            (1, "2021-02-29", "Pfizer",             "2021-03-01 00:00:00.0",        2,      9),
            (1, "2021-02-28", "Pfizer",             "2021-03-01 00:00:00.0",        3,      7),
            (2, "2021-01-01", "Pfizer/AZ/Moderna",  "2021-01-02 00:00:00.0",        1,      9),
            (2, "2021-01-29", "Pfizer/AZ/Moderna",  "2021-02-03 00:00:00.0",        2,      1),
            (2, "2021-02-28", "Pfizer/AZ/Moderna",  "2021-03-04 00:00:00.0",        3,      1),
            (3, "2021-01-01", "Other",              "2021-01-03 00:00:00.0",        1,      9),
            (3, "2021-03-26", "Other",              "2021-03-27 00:00:00.0",        2,      7),
            (3, "2021-04-14", "Other",              "2021-04-16 00:00:00.0",        3,      7),
        ],
        # fmt: on
        schema="id integer, vaccine_date string, vaccine_type string, visit_datetime string, doses integer, order integer",
    )
    for col in ["vaccine_date", "visit_datetime"]:
        expected_df = expected_df.withColumn(col, F.to_timestamp(col))
    output_df = assign_valid_order(
        df=expected_df.drop("order"),
        column_name_to_assign="order",
        participant_id_column="id",
        vaccine_date_column="vaccine_date",
        vaccine_type_column="vaccine_type",
        visit_datetime_column="visit_datetime",
    )
    assert_df_equality(output_df, expected_df, ignore_nullable=True, ignore_row_order=False, ignore_column_order=False)


def test_assign_valid_order_2(spark_session):
    expected_df = spark_session.createDataFrame(
        # fmt: off
        data=[
            (1, "2021-01-01", "Pfizer",             "2021-01-02 00:00:00.0",        1,      9),
            (1, "2021-01-30", "Pfizer",             "2021-02-02 00:00:00.0",        2,      3),
            (1, "2021-02-28", "Pfizer",             "2021-03-01 00:00:00.0",        3,      7),
            (2, "2021-01-01", "Pfizer/AZ/Moderna",  "2021-01-02 00:00:00.0",        1,      9),
            (2, "2021-01-29", "Pfizer/AZ/Moderna",  "2021-02-03 00:00:00.0",        2,      1),
            (2, "2021-02-28", "Pfizer/AZ/Moderna",  "2021-03-04 00:00:00.0",        3,      1),
            (3, "2021-01-01", "Other",              "2021-01-03 00:00:00.0",        1,      9),
            (3, "2021-03-26", "Other",              "2021-03-27 00:00:00.0",        2,      7),
            (3, "2021-04-14", "Other",              "2021-04-16 00:00:00.0",        3,      7),
        ],
        # fmt: on
        schema="id integer, vaccine_date string, vaccine_type string, visit_datetime string, doses integer, order integer",
    )
    for col in ["vaccine_date", "visit_datetime"]:
        expected_df = expected_df.withColumn(col, F.to_timestamp(col))

    output_df = update_valid_order_2(
        df=expected_df,
        column_name_to_update="order",
        participant_id_column="id",
        vaccine_date_column="vaccine_date",
        vaccine_type_column="vaccine_type",
        valid_order_column="order",
        visit_datetime_column="visit_datetime",
        vaccine_number_doses_column="doses",
    )
    assert_df_equality(
        output_df,
        expected_df,
        ignore_nullable=True,
        ignore_row_order=True,
        ignore_column_order=True,
    )
