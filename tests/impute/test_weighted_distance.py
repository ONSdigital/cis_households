from chispa import assert_df_equality
from pyspark.sql import functions as F

from cishouseholds.impute import weighted_distance


def test_weighted_distance(spark_session):
    """
    Test that the only records with a minimum distance are kept within each group.
    """
    df = spark_session.createDataFrame(
        data=[
            ("1", "g1", 1, 1, 0.0),
            ("2", "g2", 1, 2, 10.0),
            ("3", "g3", 3, 3, 0.0),
            ("4", "g3", 3, 4, 10.0),
            ("5", "g4", None, None, 0.0),
        ],
        schema="uid string, group_id string, example integer, don_example integer, distance double",
    )
    df_input = df.drop("distance")
    expected_df = df.where(F.col("uid") != "4")

    actual_df = weighted_distance(df_input, "group_id", ["example"], [10])
    assert_df_equality(actual_df, expected_df, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)
