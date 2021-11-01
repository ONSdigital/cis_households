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
            (7, "NI", 3),
            (8, "Scotland", 2),
            (9, "Scotland", None),
            (10, "England", None),
            (11, "NI", None),
        ],
        schema="id integer, country string, school_year integer",
    )
    split_school_year_by_country(input_df, "school_year", "country", "id")
