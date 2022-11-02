import functools
import os
from typing import Any
from typing import List
from typing import Union

import yaml

from cishouseholds.hdfs_utils import read_file_to_string


@functools.lru_cache(maxsize=1)
def get_config() -> dict:
    """Read YAML config file from path specified in PIPELINE_CONFIG_LOCATION environment variable"""
    configs: List[Any] = []
    modified_config = {}

    for conifg_location_name in ["PIPELINE_CONFIG_LOCATION", "MASTER_CONFIG_LOCATION"]:
        config_location = os.environ.get(conifg_location_name)
        if config_location is None:
            configs.append({})
            print(
                f"WARNING: {conifg_location_name} environment variable should be set to "
                "the config file path or passed to `run_from_config`."
                " An empty dictionary will be used by default for this run."
            )  # functional
        else:
            configs.append(get_secondary_config(config_location))

    modified_config = {k: v for k, v in configs[1].items()}
    for section_name, section_config in configs[0].items():
        if type(section_config) == dict and section_name != "run":
            for key, val in section_config.items():
                if key in modified_config[section_name] and type(modified_config[section_name][key]) == dict:
                    modified_config[section_name][key].update(val)
                else:
                    modified_config[section_name][key] = val

        else:
            modified_config[section_name] = section_config

    return modified_config


def get_secondary_config(location) -> Union[dict, None]:  # type: ignore
    if location is None:
        return None
    if location[:8] == "hdfs:///":
        config = yaml.safe_load(read_file_to_string(location))
    else:
        with open(location) as fh:
            config = yaml.load(fh, Loader=yaml.FullLoader)
    return config


#
