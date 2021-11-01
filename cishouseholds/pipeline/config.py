import functools
import os

import yaml


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
