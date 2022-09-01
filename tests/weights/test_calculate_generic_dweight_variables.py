import pyspark.sql.functions as F
from chispa import assert_df_equality
from pyspark.sql.window import Window

from cishouseholds.weights.design_weights import calculate_combined_design_weights


def test_calculate_generic_design_weight_variables(spark_session):
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
                0.5,
            ),
            (
                "A",
                2,
                1,
                1,
                1.0,
            ),
        ],
        schema="""
           groupby string,
           weight integer,
           window integer,
           number_eligible_household_sample long,
           combined_design_weight_type1 double
            """,
    )
    window = Window.partitionBy("window")
    output_df = calculate_combined_design_weights(
        df=input_df,
        column_name_to_assign="combined_design_weight_type1",
        design_weight_column="weight",
        groupby_columns=["groupby"],
        eligible_household_count_column="number_eligible_household_sample",
        cis_window=window,
    )

    assert_df_equality(output_df, expected_df, ignore_column_order=True, ignore_row_order=True, ignore_nullable=True)
