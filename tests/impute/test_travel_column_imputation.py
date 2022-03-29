from chispa import assert_df_equality

from cishouseholds.edit import update_travel_column
from cishouseholds.impute import fill_forward_from_last_change


def test_travel_column_imputation(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            (1, "2021-01-01", "CountryA", "2020-09-01", None),
            (1, "2021-01-02", None, None, "No"),
            (1, "2021-01-03", None, None, "No"),
            (1, "2021-01-04", None, None, None),
            (1, "2021-01-05", None, None, "No"),
            (1, "2021-01-06", "CountryA", "2020-09-01", None),
            (1, "2021-01-07", None, None, "No"),
            (1, "2021-01-08", None, None, None),
            (1, "2021-01-09", None, None, "No"),
            (2, "2021-01-01", None, None, "No"),
            (2, "2021-01-02", None, None, None),
            (2, "2021-01-03", None, None, "No"),
            (2, "2021-01-04", None, None, None),
            (2, "2021-01-05", None, None, "No"),
            (2, "2021-01-06", None, None, None),
            (2, "2021-01-07", None, None, "No"),
            (2, "2021-01-08", None, None, "No"),
            (2, "2021-01-09", None, None, None),
            (2, "2021-01-10", None, None, None),
            (2, "2021-01-11", "CountryB", "2020-10-20", "Yes"),
            (2, "2021-01-12", None, None, None),
            (2, "2021-01-13", "CountryB", "2020-10-20", "Yes"),
            (2, "2021-01-14", None, None, None),
            (3, "2021-01-01", "CountryC", "2020-09-30", None),
            (3, "2021-01-02", "CountryC", "2020-10-01", None),
            (3, "2021-01-03", None, None, "No"),
            (3, "2021-01-04", None, None, None),
            (3, "2021-01-05", None, None, "No"),
            (4, "2021-01-01", None, None, "No"),
            (4, "2021-01-02", "CountryD", "2020-03-01", "Yes"),
            (4, "2021-01-03", None, None, None),
            (4, "2021-01-04", "CountryD", "2020-03-01", "Yes"),
            (4, "2021-01-05", None, None, None),
            (4, "2021-01-06", None, None, "No"),
        ],
        schema="id integer,date string,Been_outside_uk_last_country string,Been_outside_uk_last_date string,Been_outside_uk_since string",
    )

    expected_df = spark_session.createDataFrame(
        data=[
            # fmt: off
            (1, "2021-01-01", "CountryA", "2020-09-01", "Yes"),
            (1, "2021-01-02", "CountryA", "2020-09-01", "Yes"),
            (1, "2021-01-03", "CountryA", "2020-09-01", "Yes"),
            (1, "2021-01-04", "CountryA", "2020-09-01", "Yes"),
            (1, "2021-01-05", "CountryA", "2020-09-01", "Yes"),
            (1, "2021-01-06", "CountryA", "2020-09-01", "Yes"),
            (1, "2021-01-07", "CountryA", "2020-09-01", "Yes"),
            (1, "2021-01-08", "CountryA", "2020-09-01", "Yes"),
            (1, "2021-01-09", "CountryA", "2020-09-01", "Yes"),
            (2, "2021-01-01", None, None, "No"),
            (2, "2021-01-02", None, None, "No"),
            (2, "2021-01-03", None, None, "No"),
            (2, "2021-01-04", None, None, "No"),
            (2, "2021-01-05", None, None, "No"),
            (2, "2021-01-06", None, None, "No"),
            (2, "2021-01-07", None, None, "No"),
            (2, "2021-01-08", None, None, "No"),
            (2, "2021-01-09", None, None, "No"),
            (2, "2021-01-10", None, None, "No"),
            (2, "2021-01-11", "CountryB", "2020-10-20", "Yes"),
            (2, "2021-01-12", "CountryB", "2020-10-20", "Yes"),
            (2, "2021-01-13", "CountryB", "2020-10-20", "Yes"),
            (2, "2021-01-14", "CountryB", "2020-10-20", "Yes"),
            (3, "2021-01-01", "CountryC", "2020-09-30", "Yes"),
            (3, "2021-01-02", "CountryC", "2020-10-01", "Yes"),
            (3, "2021-01-03", "CountryC", "2020-10-01", "Yes"),
            (3, "2021-01-04", "CountryC", "2020-10-01", "Yes"),
            (3, "2021-01-05", "CountryC", "2020-10-01", "Yes"),
            (4, "2021-01-01", None, None, "No"),
            (4, "2021-01-02", "CountryD", "2020-03-01", "Yes"),
            (4, "2021-01-03", "CountryD", "2020-03-01", "Yes"),
            (4, "2021-01-04", "CountryD", "2020-03-01", "Yes"),
            (4, "2021-01-05", "CountryD", "2020-03-01", "Yes"),
            (4, "2021-01-06", "CountryD", "2020-03-01", "Yes"),
            # fmt: on
        ],
        schema="id integer,date string,Been_outside_uk_last_country string,Been_outside_uk_last_date string,Been_outside_uk_since string",
    )
    input_df = update_travel_column(
        input_df, "Been_outside_uk_since", "Been_outside_uk_last_country", "Been_outside_uk_last_date"
    )
    actual_df = fill_forward_from_last_change(
        df=input_df,
        fill_forward_columns=["Been_outside_uk_since", "Been_outside_uk_last_country", "Been_outside_uk_last_date"],
        participant_id_column="id",
        visit_date_column="date",
        record_changed_column="Been_outside_uk_since",
        record_changed_value="Yes",
    )
    assert_df_equality(actual_df, expected_df, ignore_row_order=False, ignore_column_order=True)
