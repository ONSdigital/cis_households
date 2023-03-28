import pyspark.sql.functions as F
from chispa import assert_df_equality

from cishouseholds.derive import assign_valid_order
from cishouseholds.edit import update_valid_order_2


def test_assign_valid_order(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            (1, "2021-01-01", "Pfizer", "Yes", 9),
            (1, "2021-02-01", "Pfizer", "No", 3),
            (2, "2021-01-01", "Pfizer/AZ/Moderna", "Yes", 9),
            (2, "2021-02-01", "Pfizer/AZ/Moderna", "No", 1),
        ],
        schema="id integer, vaccine_date string, vaccine_type string, first string, order integer",
    )
    output_df = assign_valid_order(
        df=expected_df.drop("order"),
        column_name_to_assign="order",
        participant_id_column="id",
        vaccine_date_column="vaccine_date",
        vaccine_type_column="vaccine_type",
        dose_column="first",
    )
    assert_df_equality(output_df, expected_df, ignore_nullable=True, ignore_row_order=True, ignore_column_order=True)


def test_assign_valid_order_2(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            (1, "2021-01-01", "Pfizer", "Yes", "No", 9, 9),
            (1, "2021-01-30", "Pfizer", "No", "Yes", 3, 3),  #
            (1, "2021-02-28", "Pfizer", "No", "No", 3, 3),  #
            (2, "2021-01-01", "Pfizer/AZ/Moderna", "Yes", "No", 9, 9),
            (2, "2021-01-30", "Pfizer/AZ/Moderna", "No", "Yes", 1, 1),
            (2, "2021-02-28", "Pfizer/AZ/Moderna", "No", "No", 1, 1),
            (3, "2021-01-01", "Other", "Yes", "No", 7, 7),
            (3, "2021-01-30", "Other", "No", "Yes", 7, 7),
            (3, "2021-02-28", "Other", "No", "No", 7, 7),
        ],
        schema="id integer, vaccine_date string, vaccine_type string, first string, second string, order integer, order_2 integer",
    )
    output_df = update_valid_order_2(
        df=expected_df.drop("order_2"),
        participant_id_column="id",
        valid_order_column="order",
        vaccine_date_column="vaccine_date",
        vaccine_type_column="vaccine_type",
        first_dose_column="first",
        second_dose_column="second",
    )
    assert_df_equality(
        output_df,
        expected_df.drop("order").withColumnRenamed("order_2", "order"),
        ignore_nullable=True,
        ignore_row_order=True,
        ignore_column_order=True,
    )
