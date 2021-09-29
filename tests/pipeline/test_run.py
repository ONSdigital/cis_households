import os
import pathlib
from datetime import datetime

import pandas as pd
import yaml
from chispa import assert_df_equality
from mimesis.schema import Field
from mimesis.schema import Schema

from cishouseholds.pipeline.a_test_ETL import a_test_ETL
from cishouseholds.pipeline.run import run_from_config

_ = Field("en-gb", seed=11)


def generate_test_csv(tmp_path, file_date, records):
    """
    generate unprocessed bloods data
    """
    test_description = lambda: {  # noqa E731
        "test": _("random.randint", a=1, b=9999),
    }

    schema = Schema(schema=test_description)
    test_data = pd.DataFrame(schema.create(iterations=records))

    test_data.to_csv(tmp_path / "test_data_{}.csv".format(file_date), index=False)
    return test_data


def generate_test_yaml(tmp_path: pathlib.Path, file_date: str):
    test_ETL_config = {
        "function": "a_test_ETL",
        "run": True,
        "resource_path": tmp_path / "test_data_{}.csv".format(file_date),
    }

    with open(tmp_path / "test_config.yaml", "w") as f:
        yaml.dump(test_ETL_config, f, sort_keys=False, default_flow_style=False)


def test_run_from_config(spark_session, tmp_path):
    os.environ.setdefault("CISHOUSEHOLDS_OUTPUT", str(tmp_path))
    file_date = datetime.now()
    file_date = datetime.strftime(file_date, format="%Y%m%d")
    generate_test_csv(tmp_path, file_date, 1)
    generate_test_yaml(tmp_path, file_date)
    run_from_config(tmp_path / "test_config.yaml")

    output_df = a_test_ETL(str(tmp_path / "output.csv"))
    ref_df = a_test_ETL(str(tmp_path / "test_data_{}.csv".format(file_date)))

    assert_df_equality(ref_df, output_df, ignore_row_order=True)
