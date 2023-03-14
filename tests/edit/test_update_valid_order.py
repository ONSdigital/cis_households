from chispa import assert_df_equality

from cishouseholds.edit import update_valid_order


def test_update_valid_order(spark_session):
    schema = "id integer, order string, type string, date string ,first string"
    input_df = spark_session.createDataFrame(
        data=[
            (1, 1, "Pfizer/BioNTech", "2020-08-01", "Yes"),
            (2, 1, "From a research study/trial", "2020-12-01", "Yes"),
            (2, 1, "From a research study/trial", "2022-10-01", "No"),
            (3, 1, "Oxford/AstraZeneca", "2022-08-01", "Yes"),
        ],
        schema=schema,
    )
    expected_df = spark_session.createDataFrame(
        data=[
            (1, 1.25, "Pfizer/BioNTech", "2020-08-01", "Yes"),
            (2, 1.25, "From a research study/trial", "2020-12-01", "Yes"),
            (2, 1.5, "From a research study/trial", "2022-10-01", "No"),
            (3, 1, "Oxford/AstraZeneca", "2022-08-01", "Yes"),
        ],
        schema=schema,
    )

    output_df = update_valid_order(
        df=input_df,
        column_name_to_update="order",
        participant_id_column="id",
        vaccine_type_column="type",
        vaccine_date_column="date",
        first_dose_column="first",
    )
    assert_df_equality(
        output_df,
        expected_df,
        ignore_row_order=True,
        ignore_column_order=True,
    )
