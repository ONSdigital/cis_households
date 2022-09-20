import pyspark.sql.functions as F
from chispa import assert_df_equality

from cishouseholds.derive import assign_regex_from_map
from cishouseholds.pyspark_utils import get_or_create_spark_session


def test_assign_regex_from_map(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[("AB", "A", ["B"]), ("A", "B", ["B"]), ("C", "D", None), ("A", "C", ["A"])],
        schema=["colA", "colB", "result"],
    )

    map = {"A": "A", "B": "B"}
    priority_map = {"B": 9}

    output_df = assign_regex_from_map(expected_df.drop("result"), ["result"], ["colA", "colB"], map, priority_map)
    assert_df_equality(output_df, expected_df, ignore_nullable=True)
