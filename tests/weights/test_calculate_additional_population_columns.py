from chispa import assert_df_equality

from cishouseholds.weights.population_projections import calculate_additional_population_columns


def test_calculate_additional_population_columns(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            ("England", 3, 2, 12, 12.0, None, 16.0, "missing", 1, 3),
            ("Wales", 2, 1, None, None, 6, 8.0, None, 1, 2),
        ],
        schema="""
            country string,
            age_group_swab integer,
            age_group_antibodies integer,

            p1_for_antibodies_28daysto_engl integer,
            p1_for_antibodies_evernever_engl double,
            p1_for_antibodies_wales_scot_ni integer,
            p1_for_swab_longcovid double,
            p3_for_antibodies_28daysto_engl string,

            region string,
            sex integer
            """,
    )
    output_df = calculate_additional_population_columns(
        df=expected_df,
        country_name_column="country",
        region_code_column="region",
        sex_column="sex",
        age_group_antibody_column="age_group_antibodies",
        age_group_swab_column="age_group_antibodies",
    )
    assert_df_equality(output_df, expected_df, ignore_column_order=True, ignore_row_order=True, ignore_nullable=True)
