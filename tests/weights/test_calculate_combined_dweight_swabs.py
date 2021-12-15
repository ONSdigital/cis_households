from chispa import assert_df_equality
from pyspark.sql.window import Window

from cishouseholds.weights.weights import calculate_combined_dweight_swabs


def test_calculate_combined_dweight_swabs(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            (1, 1, 1),
            (2, 1, 2),
        ],
        schema="""
            combined_design_weight_swab integer,
            number_of_households_population_by_cis integer,
            window integer
            """,
    )
    expected_df = spark_session.createDataFrame(
        data=[(2, 1, 2, 2, 0.5, 1.0), (1, 1, 1, 1, 1.0, 1.0)],
        schema="""
           combined_design_weight_swab integer,
           number_of_households_population_by_cis integer,
           window integer,
           sum_combined_design_weight_swab long,
           scaling_factor_combined_design_weight_swab double,
           scaled_design_weight_swab_nonadjusted double
        """,
    )
    window = Window.partitionBy("window")
    output_df = calculate_combined_dweight_swabs(
        df=input_df,
        design_weight_column="combined_design_weight_swab",
        num_households_column="number_of_households_population_by_cis",
        cis_window=window,
    )

    assert_df_equality(output_df, expected_df, ignore_column_order=True, ignore_row_order=True, ignore_nullable=True)
