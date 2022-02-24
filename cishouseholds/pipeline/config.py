import functools
import os
import re
from typing import Union

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


def get_hdfs_config(location) -> Union[list, dict]:
    if location is None:
        return []
    if re.match(r"^hdfs:///(.*)", location):
        yaml_string = read_file_to_string(location)
        config = yaml.load(yaml_string, Loader=yaml.FullLoader)
    else:
        with open(location) as fh:
            config = yaml.load(fh, Loader=yaml.FullLoader)

    return config
