from chispa import assert_df_equality

from cishouseholds.edit import update_column_values_from_map_condition


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
            (4,     'condition_not_met',        None),
            # fmt: on
        ],
        schema=schema,
    )

    dict_map = {
        "column_to_map":{
            'condition_met_1': 'result_1',
            'condition_met_2': 'result_2',
            'condition_met_3': 'result_3',
            'condition_met_4': 'result_4',
        },
    }

    expected_df = spark_session.createDataFrame(
        data=[
            # fmt: off
            (1,     'condition_not_met',        None),
            (2,     'condition_met_1',          'result_1'),
            (3,     'condition_met_2',          'result_2'),
            (4,     'condition_not_met',        None),
            # fmt: on
        ],
        schema=schema,
    )
    import pdb; pdb.set_trace()
    output_df = update_column_values_from_map_condition(
        df=input_df,
        condition_column='condition_column',
        column='column_to_map',
        map=dict_map,
    )
    import pdb; pdb.set_trace()
    assert_df_equality(expected_df, output_df, ignore_nullable=True)
