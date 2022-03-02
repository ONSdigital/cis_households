from chispa import assert_df_equality

from cishouseholds.impute import knn_imputation_for_date


def test_knn_imputation_for_date(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            # fmt:off
            # id,       date,               group,      group_2,    imputed_date
            (1,         '2020-01-01',       'A',        4,          '2020-01-01'),
            (2,         None,               'B',        4,          '2020-01-01'),
            # fmt:on
        ],
        schema="""
            id integer,
            date string,
            group string,
            group_2 integer,
            imputed_date string
        """,
    )

    input_df = expected_df.drop("imputed_date")

    output_df = knn_imputation_for_date(
        df=input_df,
        column_name_to_assign="imputed_date",
        reference_column="date",
        group_columns=["group", "group_2"],
        log_file_path="/",
    )
    import pdb

    pdb.set_trace()
    assert_df_equality(output_df, expected_df, ignore_row_order=True, ignore_column_order=True)
