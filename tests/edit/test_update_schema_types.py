from cishouseholds.edit import update_schema_types


def test_update_schema_types():
    input_schema = {"time_example": "string", "second_time_example": "string"}
    expected_schema = {"time_example": "test", "second_time_example": "test"}
    output_schema = update_schema_types(input_schema, ["time_example", "second_time_example"], "test")
    assert output_schema == expected_schema
