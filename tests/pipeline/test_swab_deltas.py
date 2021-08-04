import pandas as pd
import pytest
from mimesis.schema import Field
from mimesis.schema import Schema

from cishouseholds.pipeline.swab_delta_ETL import transform_swabs_delta

_ = Field("en-gb", seed=42)


@pytest.fixture
def swabs_dummy_df(spark_session):
    """
    Generate lab swabs file.
    """
    lab_swabs_description = lambda: {  # noqa: E731
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
    schema = Schema(schema=lab_swabs_description)
    pandas_df = pd.DataFrame(schema.create(iterations=5))
    spark_df = spark_session.createDataFrame(pandas_df)
    return spark_df


def test_transform_swabs_delta_ETL(swabs_dummy_df, spark_session, data_regression):
    transformed_df = transform_swabs_delta(swabs_dummy_df, spark_session).toPandas().to_dict()
    data_regression.check(transformed_df)
