from chispa import assert_df_equality
from pyspark.sql.window import Window

from cishouseholds.weights.weights import validate_design_weights_or_precal


def test_validate_design_weights(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            (1, 2.0, 2.0, 1.0, 1.0, 1),  # Fails check 1
            (2, 1.0, 2.0, 1.0, 1.0, 4),  # Fails check 4
            (2, 3.0, 2.0, 1.0, 1.0, 4),  # Fails check 4
            (3, -1.0, -1.0, 1.0, 1.0, -1),  # Fails check 2
            (4, None, None, 1.0, 1.0, None),  # Fails check 3
            (5, 2.0, 2.0, 1.0, 1.0, 2),
        ],
        schema="""
            window integer,
            weight1 double,
            weight2 double,
            swab_weight double,
            antibody_weight double,
            num_hh integer
            """,
    )
    expected_df = spark_session.createDataFrame(
        data=[
            (1, 2.0, 2.0, 1.0, 1.0, 1, False),  # Fails check 1
            (2, 1.0, 2.0, 1.0, 1.0, 4, False),  # Fails check 4
            (2, 3.0, 2.0, 1.0, 1.0, 4, False),  # Fails check 4
            (3, -1.0, -1.0, 1.0, 1.0, -1, False),  # Fails check 2
            (4, None, None, 1.0, 1.0, None, False),  # Fails check 3
            (5, 2.0, 2.0, 1.0, 1.0, 2, False),
        ],
        schema="""
            window integer,
            weight1 double,
            weight2 double,
            swab_weight double,
            antibody_weight double,
            num_hh integer,
            validated boolean
            """,
    )
    output_df = validate_design_weights_or_precal(
        df=input_df,
        num_households_column="num_hh",
        swab_weight_column="swab_weight",
        antibody_weight_column="antibody_weight",
        group_by_columns=["window"],
    )
    assert_df_equality(output_df, expected_df, ignore_column_order=True, ignore_row_order=True, ignore_nullable=True)
