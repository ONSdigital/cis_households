import pyspark.sql.functions as F
from chispa import assert_df_equality
from pyspark.sql.window import Window

from cishouseholds.weights.weights import calculate_generic_dweight_variables


def test_calculate_generic_dweight_variables(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            ("A", 1, 1, 1),
            ("A", 2, 1, 1),
        ],
        schema="""
            groupby string,
            weight integer,
            window integer,
            number_eligible_household_sample long
            """,
    )
    expected_df = spark_session.createDataFrame(
        data=[
            (
                "A",
                1,
                1,
                1,
                3,
                0.7071067811865476,
                1.5,
                0.47140452079103173,
                1.2222222222222223,
                0.8181818181818181,
                1.6363636363636362,
                0.5,
                0.5,
            ),
            (
                "A",
                2,
                1,
                1,
                3,
                0.7071067811865476,
                1.5,
                0.47140452079103173,
                1.2222222222222223,
                0.8181818181818181,
                1.6363636363636362,
                0.5,
                1.0,
            ),
        ],
        schema="""
           groupby string,
           weight integer,
           window integer,
           number_eligible_household_sample long,
           sum_raw_design_weight_type1_cis long,
           standard_deviation_raw_design_weight_type1 double,
           mean_raw_design_weight_type1 double,
           coefficient_variation_design_weight_type1 double,
           design_effect_weight_type1 double,
           effective_sample_size_design_weight_type1 double,
           sum_effective_sample_size_design_weight_type1 double,
           combining_factor_design_weight_type1 double,
           combined_design_weight_type1 double
            """,
    )
    window = Window.partitionBy("window")
    output_df = calculate_generic_dweight_variables(
        df=input_df,
        design_weight_column="weight",
        groupby_columns=["groupby"],
        test_type="type1",
        num_eligible_hosusehold_column="number_eligible_household_sample",
        cis_window=window,
    )

    assert_df_equality(output_df, expected_df, ignore_column_order=True, ignore_row_order=True, ignore_nullable=True)
