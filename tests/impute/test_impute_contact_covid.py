import pyspark.sql.functions as F
from chispa import assert_df_equality
from tkinter.messagebox import NO

from cishouseholds.impute import impute_contact_covid


def test_impute_contact_covid(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            # 0 - Check carry forward of first records date, type and contact when no contact reported on later visit
            (0, "2020-01-01", "2020-01-01", "A", None, 1),  # test fill forward date once
            (0, None, "2020-01-01", None, 1, 3),
            (1, "2020-03-12", "2020-12-20", "A", 5, 5),  # test carry forward from visit after first
            (1, None, "2020-11-20", "B", 3, 1),
            (1, "2020-02-02", "2020-01-20", "B", None, None),
            (2, None, "2020-11-20", "A", 1, 1),
            (2, "2020-04-05", "2020-12-20", None, None, 1),
            (3, "2020-05-09", "2020-12-20", "B", 2, 5),  # test no imputation cases
            (4, None, "2020-10-20", None, 1, 4),  # test type prioritization
            (4, "2020-11-09", "2020-11-20", None, 4, 3),
            (4, "2020-11-22", "2021-01-11", "A", 2, 2),
            (4, None, "2021-02-11", "B", 1, 3),
            (4, None, "2021-02-24", None, 1, 2),
        ],
        schema="id integer, date string, visit_date string, col_1 string, col_2 integer, col_3 integer",
    )
    expected_df = spark_session.createDataFrame(
        data=[
            (4, None, "2020-10-20", None, 1, 4, "No"),
            (4, "2020-11-09", "2020-11-20", None, 4, 3, "Yes"),
            (4, "2020-11-22", "2021-01-11", "A", 2, 2, "Yes"),
            (4, "2020-11-22", "2021-02-11", "A", 2, 2, "Yes"),
            (4, "2020-11-22", "2021-02-24", "A", 2, 2, "Yes"),
            (1, "2020-03-12", "2020-11-20", "A", 5, 5, "Yes"),
            (1, "2020-03-12", "2020-12-20", "A", 5, 5, "Yes"),
            (0, None, "2020-01-01", "A", None, 1, "No"),
            (0, None, "2020-01-01", None, 1, 3, "No"),
            (3, "2020-05-09", "2020-12-20", "B", 2, 5, "Yes"),
            (2, "2020-04-05", "2020-11-20", None, None, 1, "Yes"),
            (2, "2020-04-05", "2020-12-20", None, None, 1, "Yes"),
        ],
        schema="id integer, date string, visit_date string, col_1 string, col_2 integer, col_3 integer, contact string",
    )
    for col in ["date", "visit_date"]:
        input_df = input_df.withColumn(col, F.to_timestamp(F.col(col), format="yyyy-MM-dd"))
        expected_df = expected_df.withColumn(col, F.to_timestamp(F.col(col), format="yyyy-MM-dd"))

    output_df = impute_contact_covid(
        df=input_df,
        participant_id_column="id",
        covid_date_column="date",
        visit_date_column="visit_date",
        contact_column="contact",
        carry_forward_columns=["col_1", "col_2", "col_3"],
    )
    assert_df_equality(output_df, expected_df, ignore_column_order=True, ignore_row_order=True, ignore_nullable=True)
