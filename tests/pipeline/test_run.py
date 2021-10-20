import os
import pathlib

import yaml

from cishouseholds.pipeline.pipeline_stages import register_pipeline_stage
from cishouseholds.pipeline.run import run_from_config
from cishouseholds.pyspark_utils import get_or_create_spark_session


@register_pipeline_stage("a_test_ETL")
def a_test_ETL(path: str):
    spark_session = get_or_create_spark_session()
    a_test_ETL.has_been_called = True

    return spark_session.createDataFrame([], "col string")


a_test_ETL.has_been_called = False


def generate_test_yaml(tmp_path: pathlib.Path):
    test_ETL_config = {
        "stages": [{"function": "a_test_ETL", "run": True, "path": ""}],
        "csv_output_path": tmp_path.as_posix(),
    }

    with open(tmp_path / "test_config.yaml", "w") as f:
        yaml.dump(test_ETL_config, f, sort_keys=False, default_flow_style=False)


def test_run_from_config(tmp_path):
    os.environ["PIPELINE_CONFIG_LOCATION"] = (tmp_path / "test_config.yaml").as_posix()
    generate_test_yaml(tmp_path)
    run_from_config()
    assert a_test_ETL.has_been_called
