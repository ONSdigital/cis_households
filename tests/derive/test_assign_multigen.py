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
            (2, "2020-01-22", 5, "2009-01-22", "England"),
            (3, "2021-03-11", 1, "2006-02-23", "Endland"),  # test duplicate participant id in different house
            (3, "2021-01-05", 6, "1989-03-12", "England"),
        ],
        schema="hh_id integer, visit_date string, id integer, dob string, country string",
    )
    expected_df = spark_session.createDataFrame(
        data=[
            (1, "2021-03-11", 3, "2006-02-23", "Endland", 15, None, 0),
            (1, "2022-04-05", 1, "2005-02-06", "England", 17, 12, 0),
            (1, "2020-02-06", 1, "2005-02-06", "England", 14, 10, 1),
            (2, "2020-02-05", 1, "1947-02-06", "England", 72, None, 1),
            (3, "2020-02-05", 1, "1997-11-06", "England", 22, None, 1),
            (4, "2020-12-12", 1, None, "England", None, None, 1),
            (5, "2020-01-22", 2, "2009-01-22", "England", 10, 6, 0),
            (6, "2021-01-05", 3, "1989-03-12", "England", 31, None, 0),
        ],
        schema="id integer, visit_date string, hh_id integer, dob string, country string, age_at_visit integer, school_year integer, multigen integer",
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
