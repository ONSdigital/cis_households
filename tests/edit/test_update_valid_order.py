from chispa import assert_df_equality

from cishouseholds.edit import update_valid_order


def test_update_valid_order(spark_session):
    schema = "id integer, order string, type string, date string ,date_1 string"
    input_df = spark_session.createDataFrame(
        data=[
            (1, 1, "Pfizer/BioNTech", "2020-08-01", "2020-08-01"),
            (2, 1, "From a research study/trial", "2019-08-01", "2022-08-01"),
            (3, 1, "Oxford/AstraZeneca", "2022-08-01", "2020-08-01"),
        ],
        schema=schema,
    )
    expected_df = spark_session.createDataFrame(
        data=[
            (1, 1.5, "Pfizer/BioNTech", "2020-08-01", "2020-08-01"),
            (2, 1.25, "From a research study/trial", "2019-08-01", "2022-08-01"),
            (3, 1, "Oxford/AstraZeneca", "2022-08-01", "2020-08-01"),
        ],
        schema=schema,
    )

    output_df = update_valid_order(
        df=input_df,
        column_name_to_update="order",
        participant_id_column="id",
        vaccine_type_column="type",
        vaccine_date_column_prefix="date",
    )
    assert_df_equality(
        output_df,
        expected_df,
        ignore_row_order=True,
        ignore_column_order=True,
    )
