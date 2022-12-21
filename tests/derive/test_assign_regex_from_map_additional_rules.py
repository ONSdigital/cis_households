import pyspark.sql.functions as F
from chispa import assert_df_equality

from cishouseholds.derive import assign_regex_from_map_additional_rules


def test_assign_regex_from_map_additional_rules(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            ("BX", 1, None, "BX", "BY", "BY"),
            ("A", 1, None, "A", "A", "A"),
            ("BY", 2, None, "Don't know", "Don't know", "BY"),
            ("BXY", 1, None, "BX", "BY", "BY"),
            ("C", 1, None, "Don't know", "Don't know", "Don't know"),
            ("D", 1, "D", "D", "D", "D"),
            ("E", 1, "Don't Know", "Don't Know", "Don't Know", "Don't Know"),
        ],
        schema=["colA", "colB", "output", "result1", "result2", "result3"],
    )

    map = {"A": "A", "BX": "(B|BX)", "BY": "(B|BY)", "E1": "E", "E2": "[E-G]"}
    priority_map = {"A": 3, "BX": 2, "BY": 2}
    priority_map2 = {"A": 3, "BX": 1, "BY": 2}
    value_map = {"BY": "BX"}
    conditions = {"BY": F.col("colB") == 1}

    output_df1 = assign_regex_from_map_additional_rules(
        df=expected_df,
        column_name_to_assign="output",
        reference_columns=["colA", "colB"],
        map=map,
        priority_map=priority_map,
        disambiguation_conditions=conditions,
        value_map=value_map,
        first_match_only=True,
        overwrite_values=False,
    )
    output_df2 = assign_regex_from_map_additional_rules(
        df=expected_df,
        column_name_to_assign="output",
        reference_columns=["colA", "colB"],
        map=map,
        priority_map=priority_map,
        disambiguation_conditions=conditions,
        first_match_only=True,
        overwrite_values=False,
    )
    output_df3 = assign_regex_from_map_additional_rules(
        df=expected_df,
        column_name_to_assign="output",
        reference_columns=["colA", "colB"],
        map=map,
        priority_map=priority_map2,
        disambiguation_conditions=conditions,
        first_match_only=True,
        overwrite_values=False,
    )

    assert_df_equality(
        output_df1.select("output"),
        expected_df.select("result1").withColumnRenamed("result1", "output"),
        ignore_nullable=True,
        ignore_row_order=True,
        ignore_column_order=True,
    )
    assert_df_equality(
        output_df2.select("output"),
        expected_df.select("result2").withColumnRenamed("result2", "output"),
        ignore_nullable=True,
        ignore_row_order=True,
        ignore_column_order=True,
    )
    assert_df_equality(
        output_df3.select("output"),
        expected_df.select("result3").withColumnRenamed("result3", "output"),
        ignore_nullable=True,
        ignore_row_order=True,
        ignore_column_order=True,
    )
