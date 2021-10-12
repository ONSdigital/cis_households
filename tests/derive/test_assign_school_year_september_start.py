from chispa import assert_df_equality

from cishouseholds.derive import assign_school_year_september_start


def test_assign_named_buckets(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            ("2021-07-21", "2015-02-05", 26),
            ("2021-07-21", "2015-09-05", 26),
            ("2021-07-21", "2015-08-30", 26),
            ("2021-07-21", "2005-01-05", 26),
            ("2021-07-21", "2002-11-21", 26),
            ("2021-07-21", "2012-05-05", 26),
        ],
        schema="visit_date string, dob string, school_year integer",
    )

    output_df = assign_school_year_september_start(expected_df.drop("school_year"), "dob", "visit_date", "school_year")
    assert_df_equality(output_df, expected_df)
