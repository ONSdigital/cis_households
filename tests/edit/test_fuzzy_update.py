import pyspark.sql.functions as F
from chispa.dataframe_comparer import assert_df_equality

from cishouseholds.edit import fuzzy_update


def test_fuzzy_update(spark_session):
    # fmt: off
    input_df = spark_session.createDataFrame(
        data=[
            (1, "2022-11-12", "1", "A",  "L",  1, None), # no rows exist with more than 2 matches and a not null val in update col
            (1, "2022-11-12", "3", "A",  "L",  2, None),
            (1, "2022-11-12", "4", None, "L",  3, None), # no rows exist with more than 2 matches and a not null val in update col
            (1, "2022-11-12", "4", "A",  None, 3, "update"),
            (1, "2022-11-12", "3", "B",  "L",  2, "update"),
            (1, "2022-11-12", "5", "A",  "L",  2, "update"),
            (2, "2022-11-12", "5", "A",  "L",  2, "unchanged"),
            (3, "2022-11-12", "5", "A",  "L",  2, None)
        ],
        schema="""id integer, visit_date string, A string, B string, C string, D integer, update string""",
    )
    expected_df = spark_session.createDataFrame(
        data=[
            (1, "2022-11-12", "1", "A",  "L",  1, None), # no rows exist with more than 2 matches and a not null val in update col
            (1, "2022-11-12", "3", "A",  "L",  2, "update"),
            (1, "2022-11-12", "4", None, "L",  3, None), # no rows exist with more than 2 matches and a not null val in update col
            (1, "2022-11-12", "4", "A",  None, 3, "update"),
            (1, "2022-11-12", "3", "B",  "L",  2, "update"),
            (1, "2022-11-12", "5", "A",  "L",  2, "update"),
            (2, "2022-11-12", "5", "A",  "L",  2, "unchanged"),
            (3, "2022-11-12", "5", "A",  "L",  2, None)
        ],
        schema="""id integer, visit_date string, A string, B string, C string, D integer, update string""",
    )
    # fmt: on

    output_df = fuzzy_update(
        input_df,
        cols_to_check=["A", "B", "C", "D"],
        min_matches=2,
        update_column="update",
        id_column="id",
        visit_date_column="visit_date",
        filter_out_of_range=False,
    )
    assert_df_equality(expected_df, output_df, ignore_row_order=False, ignore_column_order=False)
