from chispa import assert_df_equality

from cishouseholds.pyspark_utils import convert_array_strings_to_array
from cishouseholds.pyspark_utils import convert_array_to_array_strings


def test_convert_array_strings_to_array(spark_session):
    validation_schema = {
        "id": {"type": "integer"},
        "test_col": {"type": "array<string>"},
    }
    input_string_df = spark_session.createDataFrame(
        # fmt: off
        data=[
            (1, "item_1;item_2;item_3;item_4"),
            (2, "item_1"),
            (3, "item_3"),
            (4, "item_2;item_3"),
        ],
        # fmt: on
        schema="id integer, test_col string",
    )
    input_array_df = spark_session.createDataFrame(
        # fmt: off
        data=[
            (1, ["item_1","item_2","item_3","item_4"]),
            (2, ["item_1"]),
            (3, ["item_3"]),
            (4, ["item_2","item_3"]),
        ],
        # fmt: on
        schema="id integer, test_col array<string>",
    )
    # import pdb; pdb.set_trace()
    string_df = convert_array_to_array_strings(input_array_df)
    assert_df_equality(string_df, input_string_df, ignore_row_order=True)
    # array_df = convert_array_strings_to_array(input_string_df, validation_schema)
    # assert_df_equality(array_df, input_array_df, ignore_row_order=True)
