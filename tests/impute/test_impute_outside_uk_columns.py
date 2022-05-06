from chispa import assert_df_equality

from cishouseholds.impute import impute_outside_uk_columns


def test_impute_outside_uk_columns(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            ("2010/11/11", "France", "Yes", "2022/01/01", 0),
            (None, "France", None, "2022/01/01", 1),
            ("2020/05/21", "France", "Yes", "2022/01/01", 1),
            ("2010/05/27", "France", "No", "2022/01/01", 1),
            ("2021/07/20", "France", "Yes", "2022/01/01", 1),
            ("2010/08/13", "France", "No", "2022/01/01", 1),
        ],
        schema="outside_uk_date_column string, outside_country_column string, outside_uk_since_column string, visit_datetime_column string, id_column integer",
    )

    expected_df = spark_session.createDataFrame(
        data=[
            (None, "France", None, "2022/01/01", 1),
            ("2020/05/21", "France", "Yes", "2022/01/01", 1),
            ("2020/05/21", "France", "No", "2022/01/01", 1),
            ("2021/07/20", "France", "Yes", "2022/01/01", 1),
            ("2021/07/20", "France", "No", "2022/01/01", 1),
            (None, "France", "No", "2022/01/01", 0),
        ],
        schema="outside_uk_date_column string, outside_country_column string, outside_uk_since_column string, visit_datetime_column string, id_column integer",
    )

    actual_df = impute_outside_uk_columns(
        input_df,
        outside_country_column="outside_country_column",
        outside_uk_date_column="outside_uk_date_column",
        outside_uk_since_column="outside_uk_since_column",
        visit_datetime_column="visit_datetime_column",
        id_column="id_column",
    )
    assert_df_equality(actual_df, expected_df, ignore_row_order=True, ignore_column_order=True)
