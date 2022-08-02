from chispa import assert_df_equality
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from cishouseholds.weights.design_weights import scale_antibody_design_weights


def test_carry_forward_design_weights(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[(2, 1.0, 4, 1.0), (2, 3.0, 4, 3.0), (1, 1.0, 6, 6.0)],
        schema="""
            groupby integer,
            raw_design_weight_antibodies_ab double,
            num_hh integer,
            scaled_design_weight_antibodies_non_adjusted double
            """,
    ).withColumn(
        "scaled_design_weight_antibodies_non_adjusted",
        F.col("scaled_design_weight_antibodies_non_adjusted").cast(DecimalType(38, 20)),
    )

    output_df = scale_antibody_design_weights(
        df=expected_df.drop("scaled_design_weight_antibodies_non_adjusted"),
        column_name_to_assign="scaled_design_weight_antibodies_non_adjusted",
        design_weight_column_to_scale="raw_design_weight_antibodies_ab",
        groupby_column="groupby",
        household_population_column="num_hh",
    )
    assert_df_equality(
        output_df,
        expected_df,
        ignore_column_order=True,
        ignore_row_order=True,
        ignore_nullable=True,
    )
