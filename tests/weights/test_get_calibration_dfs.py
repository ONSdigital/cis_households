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
            (5, 5, 2, 2, 2, 5, 5, 10, 10),
        ],
        schema="""
            P11 integer,
            P12 integer,
            P9 integer,
            P6 integer,
            P1 integer,
            P2 integer,
            P3 integer,
            P7 integer,
            P5 integer
            """,
    )
    output_df1, output_df2, output_df3, output_df4, output_df5, output_df6 = get_calibration_dfs(
        df=input_df,
        country_column="country",
        age_column="age",
    )
    for df, cols in zip(
        [output_df1, output_df2, output_df3, output_df4, output_df5, output_df6],
        [["P11", "P12"], ["P9"], ["P6"], ["P1"], ["P2", "P3"], ["P7", "P5"]],
    ):
        assert_df_equality(
            df, expected_df.select(*cols), ignore_column_order=True, ignore_row_order=True, ignore_nullable=True
        )
