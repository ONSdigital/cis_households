import pyspark.sql.functions as F
from chispa import assert_df_equality

from cishouseholds.weights.derive import get_matches


def test_get_matches(spark_session):
    input_df1 = spark_session.createDataFrame(
        data=[
            ("A", 2, 1),
            ("B", 4, 2),
            ("C", 6, 4),
        ],
        schema="""
            col1 string,
            col2 integer,
            id integer
            """,
    )
    input_df2 = spark_session.createDataFrame(
        data=[
            ("A", 2, 1),
            ("Z", 4, 2),
            ("C", 7, 3),
        ],
        schema="""
            col1 string,
            col2 integer,
            id integer
            """,
    )
    expected_df = spark_session.createDataFrame(
        data=[
            (None, None, "C", None, 7, None, 3),
            (None, 1, "Z", "B", 4, 4, 2),
            (1, 1, "A", "A", 2, 2, 1),
        ],
        schema="""
            MATCHED_col1 integer,
            MATCHED_col2 integer,
            col1 string,
            col1_OLD string,
            col2 integer,
            col2_OLD integer,
            id integer
            """,
    )
    # old_sample_df: DataFrame, new_sample_df: DataFrame, selection_columns: List[str], barcode_column: str
    output_df = get_matches(
        old_sample_df=input_df1, new_sample_df=input_df2, selection_columns=["col1", "col2"], barcode_column="id"
    )

    assert_df_equality(output_df, expected_df, ignore_column_order=True, ignore_row_order=True, ignore_nullable=True)
