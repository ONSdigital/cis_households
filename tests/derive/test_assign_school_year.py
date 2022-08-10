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
        ("2021-09-01", "2014-09-01", "England",             3),
        # ("2021-08-31", "2014-09-01", "England",             2),
        # ("2021-09-01", "2014-08-31", "England",             2),
        # ("2021-08-31", "2014-08-31", "England",             3),

        # ("2021-09-01", "2017-08-31", "England",             0),

        # ("2021-09-01", "2017-09-01", "England",             None),  # too young to start school

        # ("2021-11-10", "2014-09-02", "Northern Ireland",    2),

        # ("2021-09-01", "2014-09-02", "Northern Ireland",    2), # testing edge case
        # ("2021-09-02", "2014-09-02", "Northern Ireland",    2), # day after
        # ("2021-07-02", "2014-09-02", "Northern Ireland",    2),
        # ("2021-07-03", "2014-09-02", "Northern Ireland",    2), # day after

        # ("2021-11-10", "2014-09-02", "Scotland",            2),

        # ("2021-08-15", "2014-09-02", "Scotland",            2), # testing edge case
        # ("2021-08-16", "2014-09-02", "Scotland",            2), # day after
        # ("2021-03-01", "2014-09-02", "Scotland",            2),
        # ("2021-03-02", "2014-09-02", "Scotland",            2), # day after

        # ("2021-08-15", "2014-09-02", "Wales",               1),
        # ("2021-08-15", "2014-09-02", "Northern Ireland",    1),
        # ("2021-08-15", "2014-09-02", "Scotland",            2),
        # ("2021-09-01", "2014-07-01", "Northern Ireland",    3),
        # ("2021-08-14", "2014-02-28", "Scotland",            2),
        # ("2021-08-14", "1995-02-28", "Scotland",            None),
        # ("2021-11-10", "2016-10-01", "England",             None),
        # ("2021-11-10", "2016-10-01", "Northern Ireland",    None),
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
    import pdb

    pdb.set_trace()
    assert_df_equality(actual_df, expected_df, ignore_column_order=True)
