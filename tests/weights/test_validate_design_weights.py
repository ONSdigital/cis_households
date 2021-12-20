from chispa import assert_df_equality
from pyspark.sql.window import Window

from cishouseholds.weights.weights import validate_design_weights


def test_validate_design_weights(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            (1, 2.0, 2.0, 1),  # Fails check 1
            (2, 1.0, 2.0, 4),  # Fails check 4
            (2, 3.0, 2.0, 4),  # Fails check 4
            (3, -1.0, -1.0, -1),  # Fails check 2
            (4, None, None, None),  # Fails check 3
            (5, 2.0, 2.0, 2),
        ],
        schema="""
            window integer,
            weight1 double,
            weight2 double,
            num_hh integer
            """,
    )
    expected_df = spark_session.createDataFrame(
        data=[
            (1, 2.0, 2.0, 1, "False"),  # Fails check 1
            (2, 1.0, 2.0, 4, "False"),  # Fails check 4
            (2, 3.0, 2.0, 4, "False"),  # Fails check 4
            (3, -1.0, -1.0, -1, "False"),  # Fails check 2
            (4, 0.0, 0.0, None, "False"),  # Fails check 3
            (5, 2.0, 2.0, 2, "True"),
        ],
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
        df=input_df,
        column_name_to_assign="validated",
        num_households_column="num_hh",
        window=window,
    )
    assert_df_equality(output_df, expected_df, ignore_column_order=True, ignore_row_order=True, ignore_nullable=True)
