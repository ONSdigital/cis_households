from chispa import assert_df_equality

from cishouseholds.derive import assign_valid_order


def test_assign_valid_order(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            (1, "2021-01-01", "Pfizer", "Yes", 9),
            (1, "2021-02-01", "Pfizer", "No", 3),
            (2, "2021-01-01", "Pfizer/AZ/Moderna", "Yes", 9),
            (2, "2021-02-01", "Pfizer/AZ/Moderna", "No", 1),
        ],
        schema="id integer, vaccine_date string, vaccine_type string, first string, order integer",
    )
    output_df = assign_valid_order(
        df=expected_df.drop("order"),
        column_name_to_assign="order",
        participant_id_column="id",
        vaccine_date_column="vaccine_date",
        vaccine_type_column="vaccine_type",
        first_dose_column="first",
    )
    assert_df_equality(output_df, expected_df, ignore_nullable=True, ignore_row_order=True, ignore_column_order=True)
