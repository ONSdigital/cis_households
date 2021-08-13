import pandas as pd
import pytest
from mimesis.schema import Field
from mimesis.schema import Schema

from cishouseholds.edit import rename_column_names
from cishouseholds.pipeline.input_variable_names import swab_variable_name_map
from cishouseholds.pipeline.swab_delta_ETL import swab_delta_ETL
from cishouseholds.pipeline.swab_delta_ETL import transform_swab_delta


@pytest.fixture
def swab_dummy_df(spark_session):
    """
    Generate lab swab file.
    """
    _ = Field("en-gb", seed=42)

    lab_swab_description = lambda: {  # noqa: E731
        "Sample": _("random.custom_code", mask="ONS########", digit="#"),
        "Result": _("choice", items=["Negative", "Positive", "Void"]),
        "Date Tested": _("datetime.formatted_datetime", fmt="%Y-%m-%d %H:%M:%S UTC", start=2018, end=2022),
        "Lab ID": _("choice", items=["GLS"]),
        "testKit": _("choice", items=["rtPCR"]),
        "CH1-Target": _("choice", items=["ORF1ab"]),
        "CH1-Result": _("choice", items=["Inconclusive", "Negative", "Positive", "Rejected"]),
        "CH1-Cq": _("float_number", start=10.0, end=40.0, precision=12),
        "CH2-Target": _("choice", items=["N gene"]),
        "CH2-Result": _("choice", items=["Inconclusive", "Negative", "Positive", "Rejected"]),
        "CH2-Cq": _("float_number", start=10.0, end=40.0, precision=12),
        "CH3-Target": _("choice", items=["S gene"]),
        "CH3-Result": _("choice", items=["Inconclusive", "Negative", "Positive", "Rejected"]),
        "CH3-Cq": _("float_number", start=10.0, end=40.0, precision=12),
        "CH4-Target": _("choice", items=["S gene"]),
        "CH4-Result": _("choice", items=["Positive", "Rejected"]),
        "CH4-Cq": _("float_number", start=15.0, end=30.0, precision=12),
    }
    schema = Schema(schema=lab_swab_description)
    pandas_df = pd.DataFrame(schema.create(iterations=5))

    return pandas_df


def test_transform_swab_delta_ETL(swab_dummy_df, spark_session, data_regression):
    """
    Test that swab transformation provides reproducible results.
    """
    swab_dummy_df = spark_session.createDataFrame(swab_dummy_df)
    swab_dummy_df = rename_column_names(swab_dummy_df, swab_variable_name_map)
    transformed_df = transform_swab_delta(spark_session, swab_dummy_df).toPandas().to_dict()
    data_regression.check(transformed_df)


def test_swab_delta_ETL_end_to_end(swab_dummy_df, pandas_df_to_temporary_csv):
    """
    Test that valid example data flows through the ETL from a csv file.
    """
    csv_file = pandas_df_to_temporary_csv(swab_dummy_df)
    swab_delta_ETL(csv_file.as_posix())
