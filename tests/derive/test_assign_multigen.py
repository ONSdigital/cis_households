from chispa import assert_df_equality
from pyspark.sql import functions as F

from cishouseholds.derive import assign_multigeneration

# fmt:  off


def test_assign_multigeneration(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            (1, "2020-02-06", 1, "2005-02-06", "England"),
            (1, "2020-02-05", 2, "1947-02-06", "England"),
            (1, "2022-04-05", 1, "2005-02-06", "England"),
            (1, "2020-02-05", 3, "1997-11-06", "England"),
            (1, "2020-12-12", 4, None, "England"),
            (2, "2020-01-22", 1, "2009-01-22", "England"),
            (3, "2021-03-11", 1, "2006-02-23", "Endland"),
            (3, "2021-01-05", 1, "1989-03-12", "England"),
        ],
        schema="hh_id integer, visit_date string, id integer, dob string, country string",
    )
    expected_df = spark_session.createDataFrame(
        data=[
            ("England", 1, "2020-02-05", 1, "2005-02-06", 10, 14, 1),
            ("England", 1, "2020-02-05", 2, "1947-02-06", None, 72, 1),
            ("England", 1, "2020-02-05", 1, "2005-02-06", 10, 14, 1),
            ("England", 1, "2020-02-05", 3, "1997-11-06", None, 22, 1),
            ("England", 1, "2020-02-05", 4, None, None, None, 1),
            ("England", 1, "2020-02-06", 1, "2005-02-06", 10, 14, 1),
            ("England", 1, "2020-02-06", 2, "1947-02-06", None, 72, 1),
            ("England", 1, "2020-02-06", 1, "2005-02-06", 10, 14, 1),
            ("England", 1, "2020-02-06", 3, "1997-11-06", None, 22, 1),
            ("England", 1, "2020-02-06", 4, None, None, None, 1),
            ("England", 1, "2020-12-12", 1, "2005-02-06", 11, 15, 1),
            ("England", 1, "2020-12-12", 2, "1947-02-06", None, 73, 1),
            ("England", 1, "2020-12-12", 1, "2005-02-06", 11, 15, 1),
            ("England", 1, "2020-12-12", 3, "1997-11-06", None, 23, 1),
            ("England", 1, "2020-12-12", 4, None, None, None, 1),
            ("England", 1, "2022-04-05", 1, "2005-02-06", 12, 17, 0),
            ("England", 1, "2022-04-05", 2, "1947-02-06", None, 75, 0),
            ("England", 1, "2022-04-05", 1, "2005-02-06", 12, 17, 0),
            ("England", 1, "2022-04-05", 3, "1997-11-06", None, 24, 0),
            ("England", 1, "2022-04-05", 4, None, None, None, 0),
            ("Endland", 3, "2021-01-05", 1, "2006-02-23", None, 14, 0),
            ("England", 3, "2021-01-05", 1, "1989-03-12", None, 31, 0),
            ("Endland", 3, "2021-03-11", 1, "2006-02-23", None, 15, 0),
            ("England", 3, "2021-03-11", 1, "1989-03-12", None, 31, 0),
            ("England", 2, "2020-01-22", 1, "2009-01-22", 6, 10, 0),
        ],
        schema="country string, hh_id integer, visit_date string, id integer, dob string, school_year integer, age_at_visit integer, multigen integer",
    )
    output_df = assign_multigeneration(
        df=input_df,
        column_name_to_assign="multigen",
        household_id_column="hh_id",
        participant_id_column="id",
        visit_date_column="visit_date",
        date_of_birth_column="dob",
        country_column="country",
    )
    assert_df_equality(output_df, expected_df, ignore_nullable=True, ignore_column_order=True, ignore_row_order=True)
