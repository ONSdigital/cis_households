import functools
import os
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


def get_secondary_config(location) -> Union[dict, None]:  # type: ignore
    if location is None:
        return None
    if location[:8] == "hdfs:///":
        config = yaml.safe_load(read_file_to_string(location))
    else:
        with open(location) as fh:
            config = yaml.load(fh, Loader=yaml.FullLoader)
    return config
