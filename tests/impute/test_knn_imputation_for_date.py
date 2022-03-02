from chispa import assert_df_equality
from pyspark.sql.types import StructField
from pyspark.sql.types import TimestampType

from cishouseholds.impute import impute_date_by_k_nearest_neighbours


def test_knn_imputation_for_date(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            # fmt:off
            # id,       date,               group,      group_2
            (1,         '2020-01-01',       'A',        4),
            (2,         None,               'B',        4),
            # fmt:on
        ],
        schema="""
            id integer,
            date string,
            group string,
            group_2 integer
        """,
    )
    output_df = impute_date_by_k_nearest_neighbours(
        df=expected_df,
        column_name_to_assign="date",
        donor_group_columns=["group", "group_2"],
        log_file_path="/",
    )
    assert output_df.select("date").dtypes[0][-1] == "timestamp"
