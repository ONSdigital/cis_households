import os
import shutil
from datetime import datetime

import pandas as pd
import yaml
from chispa import assert_df_equality
from mimesis.schema import Field
from mimesis.schema import Schema

from cishouseholds.pipeline.a_test_ETL import a_test_ETL
from cishouseholds.pipeline.run import run_from_config

_ = Field("en-gb", seed=11)


def generate_test_csv(directory, file_date, records):
    """
    generate unprocessed bloods data
    """
    test_description = lambda: {  # noqa E731
        "Date_Recieved": _("datetime.formatted_datetime", fmt="%Y-%m-%d %H:%M:%S UTC", start=2018, end=2022),
        "Rejection_Code": _("random.randint", a=1, b=9999),
        "Reason_for_rejection": _("text.sentence").replace(",", ";").replace('"', ""),
        "Sample_Type_V/C": _("choice", items=["V", "C"]),
    }

    schema = Schema(schema=test_description)
    test_data = pd.DataFrame(schema.create(iterations=records))

    test_data.to_csv(os.path.join(directory, f"test_data_{file_date}.csv"), index=False)
    return test_data


def generate_test_yaml(file_date: str):
    test_ETL_config = {
        "function": "test_ETL",
        "run": True,
        "resource_path": os.path.join(os.getcwd(), "tests/test_files/test_data_{}.csv".format(file_date)),
        "output_path": os.path.join(os.getcwd(), "tests/test_files"),
    }

    with open(os.path.join(os.getcwd(), "tests/test_files/test_config.yaml"), "w") as f:
        yaml.dump(test_ETL_config, f, sort_keys=False, default_flow_style=False)


def test_run_from_config(spark_session):
    file_date = datetime.now()
    test_dir = os.path.join(os.getcwd(), "tests/test_files")
    if os.path.isdir(test_dir):
        print("removing...")
        shutil.rmtree(test_dir)
    os.mkdir(test_dir)
    file_date = datetime.strftime(file_date, format="%Y%m%d")
    generate_test_csv(test_dir, file_date, 10)
    generate_test_yaml(file_date)
    run_from_config(os.path.join(os.getcwd(), "tests/test_files/test_config.yaml"))

    output_df = a_test_ETL("tests/test_files/output.csv")
    ref_df = a_test_ETL("tests/test_files/test_data_{}.csv".format(file_date))

    assert_df_equality(ref_df, output_df, ignore_row_order=True)
