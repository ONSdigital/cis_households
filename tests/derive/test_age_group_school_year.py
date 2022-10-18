from chispa import assert_df_equality

from cishouseholds.derive import assign_age_group_school_year


def test_assign_age_group_school_year(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            ("England", 6, 2, "02-6SY"),
            ("England", 4, None, "02-6SY"),
            ("Scotland", 4, None, "02-6SY"),
            ("Northern Ireland", 9, 5, "02-6SY"),
            ("Scotland", 11, 7, "07SY-11SY"),
            ("Wales", 15, 10, "07SY-11SY"),
            ("Wales", 15, 6, "07SY-11SY"),
            ("Scotland", 15, 6, None),
            ("England", 17, 12, "12SY-24"),
            ("Northern Ireland", 18, 13, "12SY-24"),
            ("England", 19, None, "12SY-24"),
            ("England", 22, None, "12SY-24"),
            ("Northern Ireland", 19, None, "12SY-24"),
            ("Northern Ireland", 22, None, "12SY-24"),
            ("England", 25, 12, "25-34"),
            ("Northern Ireland", 55, 79, "50-69"),
            ("Northern Ireland", 88, 1, "70+"),
        ],
        schema="country string, age integer, school_year integer, output string",
    )
    output_df = assign_age_group_school_year(expected_df.drop("output"), "country", "age", "school_year", "output")
    assert_df_equality(output_df, expected_df, ignore_nullable=True)
