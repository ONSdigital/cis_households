import os

import yaml

_config_location = os.environ.get("PIPELINE_CONFIG_LOCATION")
if _config_location is not None:
    with open(_config_location) as fh:
        config = yaml.load(fh, Loader=yaml.FullLoader)
else:
    config = None
