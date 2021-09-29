import os
import pathlib
from datetime import datetime

import yaml

from cishouseholds.pipeline.bloods_delta_ETL import bloods_delta_ETL
from cishouseholds.pipeline.declare_ETL import ETL_scripts
from cishouseholds.pipeline.survey_responses_version_2_ETL import survey_responses_version_2_ETL
from cishouseholds.pipeline.swab_delta_ETL import swab_delta_ETL


def run_from_config(config_location: str):
    """
    reads yaml config file containing variables (run, function and resource path) per ETL function
    requires setting of PIPELINE_CONFIG_LOCATION environment var with file path of output
    """
    with open(config_location) as fh:
        config = yaml.load(fh, Loader=yaml.FullLoader)
    for ETL in config["stages"]:
        if ETL["run"]:
            output_df = ETL_scripts[ETL["function"]](ETL["resource_path"])
            output_df.toPandas().to_csv(
                "{}/{}_output_{}.csv".format(config["csv_output_path"], ETL["function"], datetime.now()), index=False
            )


run_from_config(os.environ["PIPELINE_CONFIG_LOCATION"])
