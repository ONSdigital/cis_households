from chispa import assert_df_equality

from cishouseholds.edit import split_school_year_by_country


def test_split_school_year_by_country(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            (1, "England", 2),
            (2, "NI", 2),
            (3, "Scotland", 2),
            (4, "Wales", 1),
            (5, "NI", 1),
            (6, "Scotland", 2),
        ],
        schema="id integer, country string, school_year integer",
    )
    expected_df = spark_session.createDataFrame(
        data=[
            (1, "England", 2, None, None),
            (4, "Wales", 1, None, None),
            (3, "Scotland", None, 2, None),
            (2, "NI", None, None, 2),
            (5, "NI", None, None, 1),
            (6, "Scotland", None, 2, None),
        ],
        schema="id integer, country string, school_year_england_wales integer, \
        school_year_scotland integer,school_year_northern_ireland integer",
    )
    output_df = split_school_year_by_country(input_df, "school_year", "country", "id")
    assert_df_equality(expected_df, output_df, ignore_column_order=True, ignore_row_order=True)
