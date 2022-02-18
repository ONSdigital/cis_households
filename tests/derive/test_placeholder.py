from chispa import assert_df_equality
from msilib import schema

from cishouseholds.derive import derive_household_been_last_XX_days


def test_derive_household_been_last_XX_days(spark_session):
    schema = """
            last_XX_days string,
            last_XX_days_other_household_member string,
            household_last_XX_days string
        """
    expected_df = spark_session.createDataFrame(
        data=[
            # fmt: off
            ("Yes",     "Yes",      "Yes, I have"),
            ("Yes",     "No",       "No I haven’t, but someone else in my household has"),
            ("Yes",     None,       "No I haven’t, but someone else in my household has"),
            ("No",      "No",       "No, no one in my household has"),

            (None,      "Something",    None),
            (None,      None,           None),
            # fmt: on
        ],
        schema=schema,
    )
    input_df = expected_df.drop("household_last_XX_days")

    actual_df = derive_household_been_last_XX_days(
        df=input_df,
        household_last_XX_days="household_last_XX_days",
        last_XX_days="last_XX_days",
        last_XX_days_other_household_member="last_XX_days_other_household_member",
    )
    assert_df_equality(actual_df, expected_df, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)
