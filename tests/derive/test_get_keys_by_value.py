import pytest

from cishouseholds.derive import get_keys_by_value


@pytest.fixture
def test_data():
    return {"a": 1, "b": 2, "c": 3}


def test_get_keys_by_value(test_data):
    """Test that get_keys_by_value returns expected values given the values to lookup"""
    expected = ["a", "b"]
    actual = get_keys_by_value(test_data, values_to_lookup=[1, 2])
    assert expected == actual


def test_get_keys_by_value_throws_error_on_empty_list(test_data):
    """Test that get_keys_by_value throws a ValueError given an empty values to lookup list"""
    with pytest.raises(ValueError, match="None of the values in `values_to_lookup` are found in `input_dict`"):
        assert get_keys_by_value(test_data, values_to_lookup=[])


def test_get_keys_by_value_throws_error_on_unknown_value(test_data):
    """Test that get_keys_by_value throws a ValueError given a values to lookup list that
    contains non-existent values in test data"""
    with pytest.raises(ValueError, match="None of the values in `values_to_lookup` are found in `input_dict`"):
        assert get_keys_by_value(test_data, values_to_lookup=[99999, -1])
