from chispa import assert_df_equality
from pyspark.sql import functions as F

from cishouseholds.derive import assign_multigenerational

# fmt:  off


def test_assign_multigeneration(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            (99, "2020-09-26", 270, "1966-05-04", "England"),
            (99, "2020-09-26", 410, "2007-08-15", "England"),
            (99, "2020-09-26", 543, "2007-08-15", "England"),
            (99, "2020-09-26", 609, "2006-06-22", "England"),  # Unedited into hh_id 99
            (99, "2020-09-26", 770, "1968-03-20", "England"),
            (99, "2020-11-21", 270, "1966-05-04", "England"),
            (99, "2020-11-21", 410, "2007-08-15", "England"),
            (99, "2020-11-21", 543, "2007-08-15", "England"),
            (99, "2020-11-21", 609, "2006-06-22", "England"),  # Unedited into hh_id 99
            (99, "2020-11-21", 770, "1968-03-20", "England"),
            (20, "2022-07-06", 385, "2008-09-06", "England"),
            (20, "2022-07-06", 470, "1973-02-05", "England"),
            (99, "2022-07-06", 609, "2006-06-22", "England"),  # Manually edited into hh_id 99 from 20
            (20, "2022-07-06", 494, "1970-03-21", "England"),
            (20, "2022-08-30", 609, "2006-06-22", "England"),  # Not manually edited into hh_id 99 from 20
        ],
        schema="hh_id integer, visit_date string, id integer, dob string, country string",
    )
    expected_df = spark_session.createDataFrame(
        data=[
            (99, "2020-09-26", 270, "1966-05-04", "England", 54, None, 0),
            (99, "2020-09-26", 410, "2007-08-15", "England", 13, 9, 0),
            (99, "2020-09-26", 543, "2007-08-15", "England", 13, 9, 0),
            (99, "2020-09-26", 609, "2006-06-22", "England", 14, 10, 0),  # Unedited into hh_id 99
            (99, "2020-09-26", 770, "1968-03-20", "England", 52, None, 0),
            (99, "2020-11-21", 270, "1966-05-04", "England", 54, None, 0),
            (99, "2020-11-21", 410, "2007-08-15", "England", 13, 9, 0),
            (99, "2020-11-21", 543, "2007-08-15", "England", 13, 9, 0),
            (99, "2020-11-21", 609, "2006-06-22", "England", 14, 10, 0),  # Unedited into hh_id 99
            (99, "2020-11-21", 770, "1968-03-20", "England", 52, None, 0),
            (20, "2022-07-06", 385, "2008-09-06", "England", 13, 8, 1),
            (20, "2022-07-06", 470, "1973-02-05", "England", 49, None, 1),
            (99, "2022-07-06", 609, "2006-06-22", "England", 16, 11, 0),  # Manually edited into hh_id 99 from 20
            (20, "2022-07-06", 494, "1970-03-21", "England", 52, None, 1),
            (20, "2022-08-30", 609, "2006-06-22", "England", 16, 11, 1),  # Not manually edited into hh_id 99 from 20
        ],
        schema="hh_id integer, visit_date string, id integer, dob string, country string, age_at_visit integer, school_year integer, multigen integer",
    )

    output_df = assign_multigenerational(
        df=input_df,
        column_name_to_assign="multigen",
        household_id_column="hh_id",
        participant_id_column="id",
        visit_date_column="visit_date",
        date_of_birth_column="dob",
        country_column="country",
    )
    assert_df_equality(output_df, expected_df, ignore_nullable=True, ignore_column_order=True, ignore_row_order=True)
