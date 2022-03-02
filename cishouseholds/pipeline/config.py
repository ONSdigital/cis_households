import functools
import os

import yaml

from cishouseholds.hdfs_utils import read_file_to_string


@functools.lru_cache(maxsize=1)
def get_config() -> dict:
    """Read YAML config file from path specified in PIPELINE_CONFIG_LOCATION environment variable"""
    config_location = os.environ.get("PIPELINE_CONFIG_LOCATION")
    config = {}
    if config_location is None:
        print(
            "PIPELINE_CONFIG_LOCATION environment variable should be set to "
            "the config file path. An empty dictionary will be used by default for this run."
        )  # functional
    else:
        with open(config_location) as fh:
            config = yaml.load(fh, Loader=yaml.FullLoader)
    return config


def get_secondary_config(location) -> dict:
    if location is None:
        return {}
    if location[:8] == "hdfs:///":
        location = read_file_to_string(location)

    with open(location) as fh:
        config = yaml.load(fh, Loader=yaml.FullLoader)
    return config
