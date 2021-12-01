from chispa import assert_df_equality

from cishouseholds.pipeline.weights import calc_raw_design_weight_ab


def test_calc_raw_design_weight_ab(spark_session):
    schema_input_df = """sample_new_previous string,
                            household_designweigh_antibodies integer,
                            scaled_design_weight_swab_nonadjusted integer
                            """
    data_input_df = [("previous", 5, 3), ("previous", 2, 7), ("new", None, 9), ("new", None, None)]
    input_df = spark_session.createDataFrame(data_input_df, schema=schema_input_df)

    schema_expected_df = """sample_new_previous string,
                            household_designweigh_antibodies integer,
                            scaled_design_weight_swab_nonadjusted integer,
                            raw_design_weights_antibodies_ab integer
                            """
    data_expected_df = [("previous", 5, 3, 5), ("previous", 2, 7, 2), ("new", None, 9, 9), ("new", None, None, None)]
    expected_df = spark_session.createDataFrame(data_expected_df, schema=schema_expected_df)

    output_df = calc_raw_design_weight_ab(input_df)

    assert_df_equality(output_df, expected_df, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)
