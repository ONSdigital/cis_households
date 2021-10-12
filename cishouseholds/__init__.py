import os

import yaml

with open(os.environ["PIPELINE_CONFIG_LOCATION"]) as fh:
    config = yaml.load(fh, Loader=yaml.FullLoader)
