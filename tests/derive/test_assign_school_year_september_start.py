from chispa import assert_df_equality

from cishouseholds.derive import assign_school_year_september_start


def test_assign_school_year_september_start(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            ("2021-07-21", "2015-02-05", 1),
            ("2021-07-21", "2015-09-05", 2),
            ("2021-09-01", "2015-08-30", 3),
            ("2021-07-21", "2005-01-05", 11),
            ("2021-07-21", "2002-11-21", None),
            ("2021-07-21", "2012-05-05", 4),
        ],
        schema="visit_date string, dob string, school_year integer",
    )

    output_df = assign_school_year_september_start(expected_df.drop("school_year"), "dob", "visit_date", "school_year")
    assert_df_equality(output_df, expected_df)
