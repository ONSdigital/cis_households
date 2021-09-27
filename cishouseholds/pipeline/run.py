import os

import yaml

# import bloods_delta_ETL
# import sample_delta_ETL
# import survey_responses_version_2_ETL

# Load YAML data from the file

# Print current working directory
print("Current working dir : {}".format(os.getcwd()))

with open(os.path.join(os.getcwd(), "cishouseholds/pipeline/config.yaml")) as fh:

    read_data = yaml.load(fh, Loader=yaml.FullLoader)

print(read_data)

for ETL in read_data:
    if ETL["run"]:
        print("to run...", ETL["function"])
        ETL["function"](ETL["resource_path"])
