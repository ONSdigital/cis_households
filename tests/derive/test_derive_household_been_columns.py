from chispa import assert_df_equality

from cishouseholds.derive import derive_household_been_columns


def test_derive_household_been_columns(spark_session):
    schema = """
            individual string,
            household string,
            outcome string
        """
    expected_df = spark_session.createDataFrame(
        data=[
            # fmt: off
            ("Yes",     "Yes",      "Yes, I have"),
            ("Yes",     "No",       "Yes, I have"),
            ("Yes",     None,       "Yes, I have"),
            ("No",      "No",       "No, no one in my household has"),
            (None,      "Yes",    "No I haven’t, but someone else in my household has"),
            ("No",      "Yes",    "No I haven’t, but someone else in my household has"),
            (None,      None,    "No, no one in my household has"),
            # fmt: on
        ],
        schema=schema,
    )
    input_df = expected_df.drop("household_last_XX_days")

    actual_df = derive_household_been_columns(
        df=input_df,
        column_name_to_assign="outcome",
        individual_response_column="individual",
        household_response_column="household",
    )
    assert_df_equality(actual_df, expected_df, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)
