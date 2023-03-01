from chispa import assert_df_equality

from cishouseholds.filter import filter_single_dose


def test_filter_single_dose(spark_session):  # test function
    schema = """
    id integer, visit_datetime string, default_date int, order integer, poss string, idose integer, type string
    """
    input_df = spark_session.createDataFrame(
        data=[
            (1, "2021-07-19", 1, 1, "Yes", 2, "A"),
            (1, "2021-07-20", 0, 1, "Yes", 2, "A"),
            (2, "2021-07-20", 0, 1, "Yes", 2, "A"),
            (2, "2021-07-21", 1, 1, "Yes", 2, "A"),
            (3, "2021-07-21", 1, 1, "Yes", 2, "B"),  # disallowed type
        ],
        schema=schema,
    )
    expected_df = spark_session.createDataFrame(
        data=[
            (1, "2021-07-20", 0, 1, "Yes", 2, "A"),
            (2, "2021-07-20", 0, 1, "Yes", 2, "A"),
        ],
        schema=schema,
    )
    output_df = filter_single_dose(
        df=input_df,
        participant_id_column="id",
        visit_datetime_column="visit_datetime",
        order_column="order",
        i_dose_column="idose",
        poss_1_2_column="poss",
        default_date_column="default_date",
        vaccine_type_column="type",
        allowed_vaccine_types=["A"],
    )

    assert_df_equality(output_df, expected_df, ignore_row_order=True, ignore_column_order=True)
