from chispa import assert_df_equality

from cishouseholds.weights.derive import assign_ethnicity_white


def test_assign_ethnicity_white(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            ("Yes", "England", "white", "white british"),
            ("Yes", "Wales", "white", "other white"),
            ("No", "Norther Ireland", "black", "black"),
        ],
        schema="""
            ethnicity_white string,
            country string,
            ni_ethnicity string,
            ethnicity string
            """,
    )
    output_df = assign_ethnicity_white(
        df=expected_df.drop("ethnicity_white"),
        column_name_to_assign="ethnicity_white",
        country_column="country",
        ethnicity_column_ni="ni_ethnicity",
        ethnicity_column="ethnicity",
    )
    assert_df_equality(output_df, expected_df, ignore_column_order=True, ignore_row_order=True, ignore_nullable=True)
