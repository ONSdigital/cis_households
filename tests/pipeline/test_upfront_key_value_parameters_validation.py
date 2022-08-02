import pytest

from cishouseholds.validate import ConfigError
from cishouseholds.validate import validate_config_stages


def test_upfront_key_value_parameters_validation_pass():
    def mock_function_a(input_1, input_2):
        return input_1, input_2

    mock_pipeline_stages = {
        "mock_function_a": mock_function_a,
        "mock_function_aa": mock_function_a,
        "mock_function_b": mock_function_a,
        "mock_function_c": mock_function_a,
    }
    mock_config = [
        {
            "function": "mock_function_a",
            "run": True,
            "input_1": "",
            "input_2": "",
        },
        {
            "function": "mock_function_aa",
            "run": True,
            "input_1": "",
            "input_2": "",
        },
        {
            "function": "mock_function_b",
            "run": False,
            "input_1": "",
            "input_2": "",
        },
        {
            "function": "mock_function_c",
            "run": True,
            "input_1": "",
            "input_2": "",
            "when": {
                "operator": "all",  # all functions must be set as true
                "conditions": {
                    "mock_function_a": "updated",
                    "mock_function_aa": "updated",
                },
            },
        },
        {
            "function": "mock_function_c",
            "run": True,
            "input_1": "",
            "input_2": "",
            "when": {
                "operator": "any",  # either of the functions must be set as true
                "conditions": {
                    "mock_function_a": "updated",
                    "mock_function_b": "No files",
                },
            },
        },
    ]
    validate_config_stages(all_object_function_dict=mock_pipeline_stages, config_arguments_list_of_dict=mock_config)


def test_upfront_key_value_parameters_validation_fail():
    def mock_function_a(input_1, input_2):
        return input_1, input_2

    def mock_function_b(input_1, input_2):
        return input_1, input_2

    mock_function_c = mock_function_b

    mock_pipeline_stages = {
        "mock_function_a": mock_function_a,
        "mock_function_b": mock_function_b,
        "mock_function_c": mock_function_c,
        "mock_function_d": mock_function_c,
    }
    mock_config = [
        {
            "function": "mock_function_a",  # function lacking inputs
            "run": True,
            "input_1": "",
            # 'input_2': '', # needed parameter
        },
        {
            "function": "mock_function_b",  # function with extra inputs
            "run": True,
            "input_1": "",
            "input_2": "",
            "unwanted_parameter": True,
        },
        {
            "function": "mock_function_a",
            "run": 1,  # test whether the validator finds the unacceptable run value not beeing bool and passing as true
            "input_1": "",
            "input_2": "",
        },
        {
            "function": "mock_function_b",
            "run": "a",  # test whether the validator finds the unacceptable run value not beeing bool
            "input_1": "",
            "input_2": "",
        },
        {
            "function": "mock_function_c",
            "run": False,
            "input_1": "",
            "input_2": "",
            "unwanted_parameter": True,  # despite the function not being run (run=False), validate its parameters
        },
        {
            "function": "mock_function_d",
            "run": False,
            "input_1": "",
            "input_2": "",
            "when": {"operator": "all", "conditions": {"mock_function_c": "updated"}},
        },
    ]
    with pytest.raises(ConfigError) as config_error:
        validate_config_stages(all_object_function_dict=mock_pipeline_stages, config_arguments_list_of_dict=mock_config)
    assert all(
        [
            message in str(config_error.value)
            for message in [
                "- Run parameter in mock_function_a has to be boolean type instead of <class 'int'>.",
                "- Run parameter in mock_function_b has to be boolean type instead of <class 'str'>.",
                "- mock_function_a stage does not have in the config file: input_2.",
                "- mock_function_b stage have unrecognised as input arguments: unwanted_parameter.",
                "- mock_function_c stage have unrecognised as input arguments: unwanted_parameter.",
                "- mock_function_d stage requires mock_function_c stage to be turned as True.",
            ]
        ]
    )
