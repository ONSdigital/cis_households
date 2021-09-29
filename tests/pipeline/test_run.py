import os
import pathlib
from datetime import datetime

import pandas as pd
import yaml
from chispa import assert_df_equality

from cishouseholds.pipeline.a_test_ETL import a_test_ETL
from cishouseholds.pipeline.run import run_from_config


def generate_test_yaml(tmp_path: pathlib.Path):
    test_ETL_config = {
        "stages": [{"function": "a_test_ETL", "run": True, "resource_path": ""}],
        "csv_output_path": tmp_path.as_posix(),
    }

    with open(tmp_path / "test_config.yaml", "w") as f:
        yaml.dump(test_ETL_config, f, sort_keys=False, default_flow_style=False)


def test_run_from_config(spark_session, tmp_path):
    os.environ["PIPELINE_CONFIG_LOCATION"] = (tmp_path / "test_config.yaml").as_posix()
    generate_test_yaml(tmp_path)
    output_file = run_from_config()
    assert a_test_ETL.has_been_called
