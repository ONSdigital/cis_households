import functools
import os

import yaml


@functools.lru_cache(maxsize=1)
def get_config() -> dict:
    """Read YAML config file from path specified in PIPELINE_CONFIG_LOCATION environment variable"""
    _config_location = os.environ.get("PIPELINE_CONFIG_LOCATION")
    if _config_location is None:
        raise ValueError("PIPELINE_CONFIG_LOCATION environment variable must be set to the config file path")
    with open(_config_location) as fh:
        config = yaml.load(fh, Loader=yaml.FullLoader)
    return config
