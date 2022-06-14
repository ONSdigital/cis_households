from chispa import assert_df_equality

from cishouseholds.derive import assign_named_buckets


def test_assign_named_buckets(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            ("a", 6, "2-11"),
            ("b", 11, "2-11"),
            ("c", 18, "12-19"),
            ("d", 19, "12-19"),
            ("e", 20, "20-49"),
            ("f", 55, "50-69"),
            ("g", 70, "70+"),
            ("h", 0, None),
        ],
        schema="name string, age integer, age_range string",
    )
    map = {2: "2-11", 12: "12-19", 20: "20-49", 50: "50-69", 70: "70+"}
    output_df = assign_named_buckets(expected_df.drop("age_range"), "age", "age_range", map)
    assert_df_equality(output_df, expected_df)
