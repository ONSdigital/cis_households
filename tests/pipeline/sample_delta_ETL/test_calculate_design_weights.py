from chispa import assert_df_equality

from cishouseholds.pipeline.sample_delta_ETL import calculate_design_weights


def test_calculate_design_weights(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            ("1", 1, 2.0),
            ("2", 2, 5.0),
            ("3", 2, 5.0),
        ],
        schema="uac string, interim_id integer, design_weight double",
    )
    input_df = expected_df.drop("design_weight")

    populations_df = spark_session.createDataFrame(
        data=[
            (1, 2),
            (2, 10),
        ],
        schema="interim_id integer, nb_addresses integer",
    )

    output_df = calculate_design_weights(input_df, populations_df)

    assert_df_equality(output_df, expected_df, ignore_column_order=True, ignore_row_order=True)
