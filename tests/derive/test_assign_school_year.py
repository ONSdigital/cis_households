import pyspark.sql.functions as F
import pytest
from chispa import assert_df_equality

from cishouseholds.derive import assign_school_year


@pytest.fixture
def school_year_lookup(spark_session):
    return spark_session.createDataFrame(
        data=[
            # fmt: off
            ("England",             "09", "01",     "09", "01"),
            ("Wales",               "09", "01",     "09", "01"),
            ("Scotland",            "08", "15",     "03", "01"),
            ("Northern Ireland",    "09", "01",     "07", "02"),
            # fmt: on
        ],
        schema=["country", "school_start_month", "school_start_day", "school_year_ref_month", "school_year_ref_day"],
    )


@pytest.mark.parametrize(
    "expected_data",
    # fmt: off
    [
        ("2021-09-01", "2014-09-01", "England",             2),
        ("2021-08-31", "2014-09-01", "England",             1), # new reference cutoff not started and child born after start date
        ("2021-09-01", "2014-08-31", "England",             3), # new reference cutoff started and child born before start date
        ("2021-08-31", "2014-08-31", "England",             2), # new reference cutoff not started and child born before start date
        ("2021-09-01", "2017-08-31", "England",             0), # reception

        ("2016-09-01", "1999-07-20", "England",             13), # stefens date
        ("2016-09-01", "1999-08-31", "Wales",             13), # stefens date
        ("2016-09-01", "1998-09-01", "Wales",             13), # stefens date

        ("2020-09-01", "2003-08-31", "Wales",             13), # leap
        ("2020-09-01", "2002-09-01", "Wales",             13),

        ("2021-09-01", "2004-08-31", "Wales",             13), # non leap
        ("2021-09-01", "2003-09-01", "Wales",             13),

        ("2022-09-01", "2003-08-31", "Wales",             13), # non leap
        ("2022-09-01", "2004-09-01", "Wales",             13),

        ("2024-09-01", "2005-08-31", "Wales",             13), # leap
        ("2024-09-01", "2006-09-01", "Wales",             13),

        ("2021-09-01", "2017-09-01", "England",             None),  # too young to start school

        ("2021-08-15", "2014-08-15", "Scotland",            2), # testing edge case after school start
        ("2021-08-14", "2014-08-15", "Scotland",            1), # new school year not yet started and child born after start date
        ("2021-08-15", "2014-08-14", "Scotland",            2), # new school year started and child born before start date
        ("2021-08-14", "2014-08-14", "Scotland",            1), # new school year not yet started and child born before start date

        ("2021-03-01", "2014-03-01", "Scotland",            1), # testing edge case after date allocation cut off
        ("2021-02-28", "2014-03-01", "Scotland",            1), # new reference cutoff not started and child born after reference date
        ("2021-03-01", "2014-02-28", "Scotland",            2), # new reference cutoff started and child born before reference date
        ("2021-02-28", "2014-02-28", "Scotland",            2), # new reference cutoff not started and child born before reference date

        ("2021-08-15", "2014-02-28", "Scotland",            3), # new school year started and child born before reference cutoff date
        ("2021-08-15", "2014-03-01", "Scotland",            2), # new school year started and child born after reference cutoff date

        ("2021-09-01", "2015-09-01", "Northern Ireland",    0), # reception

        ("2021-07-02", "2014-07-02", "Northern Ireland",    1), # testing edge case after date allocation cut off
        ("2021-07-01", "2014-07-02", "Northern Ireland",    1), # new reference cutoff not started and child born after reference date
        ("2021-07-02", "2014-07-01", "Northern Ireland",    2), # new reference cutoff started and child born before reference date
        ("2021-07-01", "2014-07-01", "Northern Ireland",    2), # new reference cutoff not started and child born before reference date

        ("2021-07-01", "2003-09-01", "Northern Ireland",    13), # new reference cutoff not started and child born before reference date

        ("2021-09-01", "2003-09-01", "Wales",               12), # max age to be in school
        ("2021-09-01", "2003-09-02", "Wales",               13), # max age to be in school
        ("2021-09-01", "2003-08-30", "Wales",               None), # one day older to be in school
    ],
    # fmt: on
)
def test_assign_school_year(spark_session, expected_data, school_year_lookup):

    expected_schema = "visit_date string, dob string, country string, school_year integer"

    expected_df = (
        spark_session.createDataFrame([expected_data], schema=expected_schema)
        # to_timestamp will not work for creation of test data, unix approach preferred
        .withColumn("visit_date", F.to_date(F.col("visit_date"))).withColumn("dob", F.to_date(F.col("dob")))
    )

    input_df = expected_df.drop("school_year")
    actual_df = assign_school_year(input_df, "school_year", "visit_date", "dob", "country", school_year_lookup)

    assert_df_equality(actual_df, expected_df, ignore_column_order=True)
