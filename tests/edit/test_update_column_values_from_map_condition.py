from chispa import assert_df_equality

from cishouseholds.edit import update_column_values_from_map

# from cishouseholds.edit import update_column_values_from_map_condition


def test_update_column_values_from_map_condition(spark_session):
    schema = """
            id integer,
            condition_column string,
            column_to_map string
        """
    input_df = spark_session.createDataFrame(
        data=[
            # fmt: off
            (1,     'condition_not_met',        'something_else'),
            (2,     'condition_met_1',          'something_else'),
            (3,     'condition_met_2',          'something_else'),
            (4,     'condition_not_met',        'something_else'),
            (5,     None,                       None),
            # fmt: on
        ],
        schema=schema,
    )

    dict_map = {
        "column_to_map": {
            "condition_met_1": "result_1",
            "condition_met_2": "result_2",
            "condition_met_3": "result_3",
            "condition_met_4": "result_4",
            None: "filled",
        },
    }

    expected_df = spark_session.createDataFrame(
        data=[
            # fmt: off
            (1,     'condition_not_met',        'something_else'),
            (2,     'condition_met_1',          'result_1'),
            (3,     'condition_met_2',          'result_2'),
            (4,     'condition_not_met',        'something_else'),
            (5,      None,                      'filled'),
            # fmt: on
        ],
        schema=schema,
    )

    output_df = update_column_values_from_map(
        df=input_df, reference_column="condition_column", column="column_to_map", map=dict_map["column_to_map"]
    )
    assert_df_equality(expected_df, output_df, ignore_nullable=True, ignore_column_order=True, ignore_row_order=True)
