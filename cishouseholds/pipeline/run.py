import os

import yaml
from bloods_delta_ETL import bloods_delta_ETL  # noqa F401
from sample_delta_ETL import sample_delta_ETL  # noqa F401
from survey_responses_version_2_ETL import survey_responses_version_2_ETL  # noqa F401

# Load YAML data from the file

# Print current working directory


def run_from_config(config_location: str):
    with open(config_location) as fh:

        read_data = yaml.load(fh, Loader=yaml.FullLoader)

    print(read_data)
    print("locales: ", locals())

    for ETL in read_data:
        if ETL["run"]:
            locals()[ETL["function"]](ETL["resource_path"])


run_from_config(os.path.join(os.getcwd(), "cishouseholds/pipeline/config.yaml"))
