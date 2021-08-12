import pyspark.sql.functions as F
import pytest
from chispa import assert_df_equality

from cishouseholds.derive import assign_school_year


@pytest.fixture
def school_year_lookup(spark_session):
    return spark_session.createDataFrame(
        data=[
            ("England", "09", "01", "09", "01"),
            ("Wales", "09", "01", "09", "01"),
            ("Scotland", "08", "15", "03", "01"),
            ("NI", "09", "01", "07", "02"),
        ],
        schema=["country", "school_start_month", "school_start_day", "school_year_ref_month", "school_year_ref_day"],
    )


@pytest.mark.parametrize(
    "expected_data",
    [
        ("2021-11-10", "2014-09-02", "England", 2),
        ("2021-11-10", "2014-09-02", "NI", 2),
        ("2021-11-10", "2014-09-02", "Scotland", 2),
        ("2021-08-15", "2014-09-02", "Wales", 1),
        ("2021-08-15", "2014-09-02", "NI", 1),
        ("2021-08-15", "2014-09-02", "Scotland", 2),
        ("2021-09-01", "2014-07-01", "NI", 3),
        ("2021-08-14", "2014-02-28", "Scotland", 2),
        ("2021-08-14", "1995-02-28", "Scotland", None),
        ("2021-11-10", "2016-10-01", "England", None),
        ("2021-11-10", "2016-10-01", "NI", None),
    ],
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
