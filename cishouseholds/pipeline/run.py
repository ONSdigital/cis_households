import os

import yaml

from cishouseholds.pipeline.a_test_ETL import a_test_ETL  # noqa F401
from cishouseholds.pipeline.bloods_delta_ETL import bloods_delta_ETL  # noqa F401
from cishouseholds.pipeline.declare_ETL import ETL_scripts
from cishouseholds.pipeline.sample_delta_ETL import sample_delta_ETL  # noqa F401
from cishouseholds.pipeline.survey_responses_version_2_ETL import survey_responses_version_2_ETL  # noqa F401


def run_from_config(config_location: str):
    """
    reads yaml config file containing variables (run, function and resource path) per ETL function
    requires setting of CISHOUSEHOLDS_OUTPUT environment var with file path of output
    """
    print("config loc: ", config_location)
    with open(config_location) as fh:
        read_data = yaml.load(fh, Loader=yaml.FullLoader)
    if type(read_data) != list:
        read_data = [read_data]
    for ETL in read_data:
        if ETL["run"]:
            print("csv path: ", ETL["resource_path"])
            output_df = ETL_scripts[ETL["function"]](ETL["resource_path"])
            output_df.toPandas().to_csv(os.path.join(os.environ["CISHOUSEHOLDS_OUTPUT"], "output.csv"), index=False)


# run_from_config(os.path.join(os.getcwd(), "cishouseholds/pipeline/config.yaml"))
