from chispa.dataframe_comparer import assert_df_equality

from cishouseholds.pipeline.post_merge_processing import impute_and_flag
from cishouseholds.pipeline.post_merge_processing import impute_by_k_nearest_neighbours


def test_impute_by_k_nearest_neighbours(spark_session):
    """Test that high level imputation fills all missing values and reduces
    to one record per participant."""
    input_data = [
        ("A", "1", "A"),
        ("B", "2", "B"),
        ("C", "1", None),
    ]
    input_df = spark_session.createDataFrame(
        input_data,
        schema="""uid string, group_column string, important_column string""",
    )

    expected_data = [
        ("A", "1", "A", 0, None),
        ("B", "2", "B", 0, None),
        ("C", "1", "A", 1, "impute_by_k_nearest_neighbours"),
    ]
    expected_df = spark_session.createDataFrame(
        expected_data,
        schema="""uid string, group_column string, important_column string, important_column_is_imputed integer, important_column_imputation_method string""",
    )

    output_df = impute_and_flag(
        input_df,
        impute_by_k_nearest_neighbours,
        reference_column="important_column",
        donor_group_columns=["group_column"],
        log_file_path="./",
    )
    assert_df_equality(
        output_df.sort("uid"),
        expected_df.sort("uid"),
        ignore_column_order=True,
    )
