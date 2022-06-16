from chispa.dataframe_comparer import assert_df_equality

from cishouseholds.pipeline.high_level_transformations import impute_and_flag
from cishouseholds.pipeline.high_level_transformations import impute_by_k_nearest_neighbours


def test_impute_by_k_nearest_neighbours(spark_session):
    """
    Test that integer and decimal part imputation runs successfully.
    """
    input_data = [
        ("A", "1", "A"),
        ("B", "1", "B"),
        ("C", "1", "C"),
        ("D", "1", "D"),
        ("E", "1", "E"),
        ("F", "1", None),
        ("G", "1", None),
        ("H", "1", None),
        ("I", "1", None),
    ]
    input_df = spark_session.createDataFrame(
        input_data,
        schema="""uid string, group_column string, important_column string""",
    )

    expected_data = [
        ("A", "1", 0, None),
        ("B", "1", 0, None),
        ("C", "1", 0, None),
        ("D", "1", 0, None),
        ("E", "1", 0, None),
        ("F", "1", 1, "impute_by_k_nearest_neighbours"),
        ("G", "1", 1, "impute_by_k_nearest_neighbours"),
        ("H", "1", 1, "impute_by_k_nearest_neighbours"),
        ("I", "1", 1, "impute_by_k_nearest_neighbours"),
    ]
    expected_df = spark_session.createDataFrame(
        expected_data,
        schema="""uid string, group_column string, important_column_is_imputed integer, important_column_imputation_method string""",
    )

    output_df = impute_and_flag(
        input_df,
        impute_by_k_nearest_neighbours,
        reference_column="important_column",
        donor_group_columns=["group_column"],
        log_file_path="./",
    )
    assert_df_equality(
        output_df.sort("uid").drop("important_column"),
        expected_df.sort("uid").drop("important_column"),
        ignore_column_order=True,
    )
