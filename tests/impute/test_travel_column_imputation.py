from chispa import assert_df_equality

from cishouseholds.edit import update_to_value_if_any_not_null
from cishouseholds.impute import fill_forward_from_last_change
from cishouseholds.pipeline.high_level_transformations import fill_forwards_travel_column

# from cishouseholds.edit import update_travel_column


def test_travel_column_imputation(spark_session):
    schema = "participant_id integer, visit_datetime string, been_outside_uk_last_country string, been_outside_uk_last_return_date string, been_outside_uk string"
    input_df = spark_session.createDataFrame(
        data=[
            # fmt: off
            # checking that if been_outside_uk_last_country and been_outside_uk_last_return_date are different, been_outside_uk has to turn into "yes"
            (1, "2021-01-01", "CountryA", "2020-09-01", None),
            (1, "2021-01-02", None, None, "No"),

            (2, "2021-01-01", None, None, "No"),
            (2, "2021-01-02", None, None, None),
            (2, "2021-01-11", "CountryB", "2020-10-20", "Yes"), # half way through fill forward
            (2, "2021-01-12", None, None, None),

            (3, "2021-01-01", "CountryC", "2020-09-30", None),
            (3, "2021-01-03", None, None, "No"),
            (3, "2021-01-04", None, None, None),

            (4, "2021-01-01", None, None, "No"),
            (4, "2021-01-02", "CountryD", "2020-03-01", "Yes"),
            (4, "2021-01-03", None, None, None),

            (5, "2021-01-02", "CountryD", None, None), # date lacking
            (5, "2021-01-03", None, None, None),

            (6, "2021-01-02", None, "2020-03-01", None), # country lacking
            (6, "2021-01-03", None, None, None),
            # TODO if the person has been outside the country before the date of the visit all those visit records have to change to yes.
            # It can be known by either showing country or date
            # (7, "2020-01-01", None, None, None),
            # (7, "2021-01-02", "CountryA", "2021-01-01", "Yes"),
            # (7, "2021-01-03", "CountryA", None, None),
            # fmt: on
        ],
        schema=schema,
    )

    expected_df = spark_session.createDataFrame(
        data=[
            # fmt: off
            (1, "2021-01-01", "CountryA", "2020-09-01", "Yes"),
            (1, "2021-01-02", "CountryA", "2020-09-01", "Yes"),

            (2, "2021-01-01", None, None, "No"),
            (2, "2021-01-02", None, None, "No"),
            (2, "2021-01-11", "CountryB", "2020-10-20", "Yes"),
            (2, "2021-01-12", "CountryB", "2020-10-20", "Yes"),

            (3, "2021-01-01", "CountryC", "2020-09-30", "Yes"),
            (3, "2021-01-03", "CountryC", "2020-09-30", "Yes"),
            (3, "2021-01-04", "CountryC", "2020-09-30", "Yes"),

            (4, "2021-01-01", None, None, "No"),
            (4, "2021-01-02", "CountryD", "2020-03-01", "Yes"),
            (4, "2021-01-03", "CountryD", "2020-03-01", "Yes"),

            (5, "2021-01-02", "CountryD", None, "Yes"),
            (5, "2021-01-03", "CountryD", None, None),

            (6, "2021-01-02", None, "2020-03-01", "Yes"),
            (6, "2021-01-03", None, "2020-03-01", "Yes"),
            # (7, "2020-01-01", None, None, None),
            # (7, "2021-01-02", "CountryA", "2021-01-01", "Yes"),
            # (7, "2021-01-03", "CountryA", "2021-01-01", "Yes"),
            # fmt: on
        ],
        schema=schema,
    )

    output_df = fill_forwards_travel_column(input_df)
    assert_df_equality(output_df, expected_df, ignore_row_order=False, ignore_column_order=True)
