from chispa import assert_df_equality
from pyspark.sql.window import Window

from cishouseholds.weights.weights import validate_design_weights


def test_validate_design_weights(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[(2, 1.0, 2.0, 4, "True"), (2, 3.0, 2.0, 4, "True"), (1, 1.0, 2.0, 6, "False")],
        schema="""
            window integer,
            weight1 double,
            weight2 double,
            num_hh integer,
            validated string
            """,
    )
    window = Window.partitionBy("window")
    output_df = validate_design_weights(
        df=expected_df.drop("validated"),
        column_name_to_assign="validated",
        num_households_column="num_hh",
        window=window,
    )
    assert_df_equality(output_df, expected_df, ignore_column_order=True, ignore_row_order=True, ignore_nullable=True)
