from cishouseholds.edit import format_string_upper_and_clean


def test_format_string_upper_and_clean():
    input = "       this is a   string          .   "
    expected = "THIS IS A STRING"
    output = format_string_upper_and_clean(input)
    assert expected == output
