from chispa import assert_df_equality
from pyspark.sql import functions as F

from cishouseholds.derive import assign_multigen


def test_assign_multigen(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            (1, "2020-02-06", 1, "2005-02-06"),
            (1, "2020-02-05", 2, "1947-02-06"),
            (1, "2021-04-05", 1, "2005-02-06"),
            (1, "2020-02-05", 3, "1997-11-06"),
            (
                1,
                "2020-12-12",
                4,
                None,
            ),
            (2, "2020-01-22", 1, "2009-01-22"),
            (3, "2021-03-11", 1, "2006-02-23"),
            (3, "2021-01-05", 1, "1989-03-12"),
        ],
        schema="hh_id integer, visit_date string, id integer, dob string",
    )
    expected_df = spark_session.createDataFrame(
        data=[
            (1, "2020-02-05", 1, "2005-02-06", 1),
            (1, "2020-02-05", 2, "1947-02-06", 1),
            (1, "2020-02-05", 1, "2005-02-06", 1),
            (1, "2020-02-05", 3, "1997-11-06", 1),
            (1, "2020-02-05", 4, None, 1),
            (1, "2020-02-06", 1, "2005-02-06", 1),
            (1, "2020-02-06", 2, "1947-02-06", 1),
            (1, "2020-02-06", 1, "2005-02-06", 1),
            (1, "2020-02-06", 3, "1997-11-06", 1),
            (1, "2020-02-06", 4, None, 1),
            (1, "2020-12-12", 1, "2005-02-06", 0),
            (1, "2020-12-12", 2, "1947-02-06", 0),
            (1, "2020-12-12", 1, "2005-02-06", 0),
            (1, "2020-12-12", 3, "1997-11-06", 0),
            (1, "2020-12-12", 4, None, 0),
            (1, "2021-04-05", 1, "2005-02-06", 1),
            (1, "2021-04-05", 2, "1947-02-06", 1),
            (1, "2021-04-05", 1, "2005-02-06", 1),
            (1, "2021-04-05", 3, "1997-11-06", 1),
            (1, "2021-04-05", 4, None, 1),
            (3, "2021-01-05", 1, "2006-02-23", 0),
            (3, "2021-01-05", 1, "1989-03-12", 0),
            (3, "2021-03-11", 1, "2006-02-23", 0),
            (3, "2021-03-11", 1, "1989-03-12", 0),
            (2, "2020-01-22", 1, "2009-01-22", 0),
        ],
        schema="hh_id integer, visit_date string, id integer, dob string, multigen integer",
    )
    output_df = assign_multigen(
        df=input_df,
        column_name_to_assign="multigen",
        hh_id_column="hh_id",
        participant_id_column="id",
        visit_date_column="visit_date",
        dob_column="dob",
    )
    assert_df_equality(output_df, expected_df, ignore_nullable=True, ignore_column_order=True, ignore_row_order=True)
