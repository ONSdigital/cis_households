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
    mock_config = {
        "mock_function_a": {
            "input_1": "",
            "input_2": "",
        },
        "mock_function_aa": {
            "input_1": "",
            "input_2": "",
        },
        "mock_function_b": {
            "input_1": "",
            "input_2": "",
        },
        "mock_function_c": {
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
        "mock_function_c": {
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
    }
    validate_config_stages(
        pipeline_stage_functions=mock_pipeline_stages,
        stages_to_run=["mock_function_a", "mock_function_aa", "mock_function_b", "mock_function_c"],
        stages_config=mock_config,
    )


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
    mock_config = {
        "mock_function_a": {
            "input_1": "",
            # 'input_2': '', # needed parameter
        },
        "mock_function_b": {  # function with extra inputs
            "input_1": "",
            "input_2": "",
            "unwanted_parameter": True,
        },
        "mock_function_c": {
            "input_1": "",
            "input_2": "",
        },
        "mock_function_custom": {  # test custom function validation
            "function": "mock_function_d",
            "input_1": "",
            "input_2": "",
            "unwanted_parameter": True,
        },
        "mock_function_d": {
            "input_1": "",
            "input_2": "",
            "when": {"operator": "all", "conditions": {"mock_function_c": "updated"}},
        },
        "mock_function_e": {  # function not running
            "input_1": "",
            "input_2": "",
            "unwanted_parameter": True,
        },
    }
    with pytest.raises(ConfigError) as config_error:
        validate_config_stages(
            pipeline_stage_functions=mock_pipeline_stages,
            stages_to_run=[
                "mock_function_a",
                "mock_function_b",
                "mock_function_c",
                "mock_function_d",
                "mock_function_custom",
            ],
            stages_config=mock_config,
        )
    assert all(
        [
            message in str(config_error.value)
            for message in [
                "- mock_function_a stage does not have in the config file: input_2.",
                "- mock_function_b stage has unrecognised input arguments: unwanted_parameter.",
                "- mock_function_d stage has unrecognised input arguments: unwanted_parameter.",
            ]
        ]
    )
