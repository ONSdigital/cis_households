from chispa import assert_df_equality

from cishouseholds.derive import map_options_to_bool_columns


def test_map_options_to_bool_columns(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            ("A", "Yes", None, None),
            ("X", None, None, None),
            ("B", None, "Yes", None),
            (None, None, None, None),
        ],
        schema="options string, A_col string, B_col string, C_col string",
    )
    output_df = map_options_to_bool_columns(
        expected_df.drop("A", "B", "C"), "options", {"A": "A_col", "B": "B_col", "C": "C_col"}
    )
    assert_df_equality(output_df, expected_df, ignore_nullable=True)
