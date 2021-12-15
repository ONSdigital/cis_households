from chispa import assert_df_equality

from cishouseholds.weights.population_projections import get_calibration_dfs


def test_get_calibration_dfs(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            ("England", 27, 12.0, 5, 1, 3, 5, 6, 7),
            ("England", 33, 11.0, 5, 2, 2, 5, 6, 7),
            ("Wales", 54, 6.0, 2, 9, 2, 3, 4, 7),
            ("Scotland", 54, 6.0, 2, 6, 2, 3, 5, 7),
            ("Northern Ireland", 54, 6.0, 2, 1, 2, 1, 6, 7),
        ],
        schema="""
            country string,
            age integer,
            p1_for_swab_longcovid double,
            population integer,
            p1_for_antibodies_wales_scot_ni integer,
            p1_for_antibodies_evernever_engl integer,
            p3_for_antibodies_28daysto_engl integer,
            p22_white_population_antibodies integer,
            p1_for_antibodies_28daysto_engl integer
            """,
    )
    expected_df = spark_session.createDataFrame(
        data=[
            ("P111", 5, "england_population_swab_longcovid"),
            ("P112", 5, "england_population_swab_longcovid"),
            ("P16", 2, "wales_population_swab_longcovid"),
            ("P16", 2, "scotland_population_swab_longcovid"),
            ("P16", 2, "northen_ireland_population_swab_longcovid"),
            ("P19", 2, "wales_population_any_antibodies"),
            ("P16", 2, "scotland_population_any_antibodies"),
            ("P11", 2, "northen_ireland_population_any_antibodies"),
            ("P13", 5, "england_population_antibodies_evernever"),
            ("P12", 5, "england_population_antibodies_evernever"),
            ("P17", 10, "england_population_antibodies_28daysto"),
            ("P35", 10, "england_population_antibodies_28daysto"),
        ],
        schema="""
            group string,
            population_total long,
            dataset_name string
            """,
    )
    output_df = get_calibration_dfs(
        df=input_df,
        country_column="country",
        age_column="age",
    )
    assert_df_equality(output_df, expected_df, ignore_column_order=True, ignore_row_order=False, ignore_nullable=True)
