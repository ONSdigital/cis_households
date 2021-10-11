import pytest
from chispa import assert_df_equality

from cishouseholds.derive import assign_age_group_school_year


@pytest.fixture
def expected_df(spark_session):
    return spark_session.createDataFrame(
        data=[
            ("England", 6, 2, "02-6SY"),
            ("NI", 9, 5, "02-6SY"),
            ("Scotland", 11, 7, "07SY-11SY"),
            ("Wales", 15, 10, "07SY-11SY"),
            ("Wales", 15, 6, "07SY-11SY"),
            ("Scotland", 15, 6, "false"),
            ("England", 17, 12, "false"),
            ("NI", 18, 13, "false"),
        ],
        schema="country string, age integer, school_year integer, output string",
    )


def test_assign_age_group_school_year(expected_df):
    output_df = assign_age_group_school_year(expected_df.drop("output"), "country", "age", "school_year", "output")
    assert_df_equality(output_df, expected_df, ignore_nullable=True)
