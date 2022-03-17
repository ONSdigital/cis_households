import pyspark.sql.functions as F
from chispa import assert_df_equality
from cis_households.cishouseholds.derive import aggregated_output_groupby
from cis_households.cishouseholds.derive import aggregated_output_window


def test_aggregated_output_window(spark_session):

    expected_df = spark_session.createDataFrame(
        data=[
            # fmt: off
            (1,     1,        1,        1,      7),
            (2,     7,        7,        7,      7),
            (2,     7,        7,        7,      7),
            (3,     0,        0,        1,      7),
            (4,     6,        1,        6,      7),
            # fmt: on
        ],
        schema="""
            id integer,
            col_1 integer,
            col_2 integer,
            col_3 integer,
            col_1_max integer
        """,
    )

    input_df = expected_df.drop("col_1_max")

    output_df = aggregated_output_window(
        df=input_df,
        column_group_list=["id"],
        apply_function_list=[F.max()],
        column_apply_list=["col_1"],
        column_name_to_assign_list=["col_1_max"],
    )
    import pdb;pdb.set_trace()
    
    assert_df_equality(expected_df, output_df, ignore_column_order=True, ignore_row_order=True)


def test_aggregated_output_groupby(spark_session):

    expected_df = spark_session.createDataFrame(
        data=[
            # fmt: off
            (),
            # fmt: on
        ],
        schema="""""",
    )

    input_df = expected_df.drop()

    output_df = aggregated_output_groupby(input_df)

    assert_df_equality(expected_df, output_df, ignore_column_order=True, ignore_row_order=True)
