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


def get_filter_config(hdfs_location) -> list:
    if hdfs_location is None:
        return []
    yaml_string = read_file_to_string(hdfs_location)
    with open(yaml_string) as fh:
        config = yaml.load(fh, Loader=yaml.FullLoader)
    return config
