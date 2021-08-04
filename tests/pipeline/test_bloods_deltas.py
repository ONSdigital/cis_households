import pandas as pd
import pytest
from mimesis.schema import Field
from mimesis.schema import Schema

from cishouseholds.pipeline.bloods_delta_ETL import transform_bloods_delta

_ = Field("en-gb", seed=42)


@pytest.fixture
def bloods_dummy_df(spark_session):
    """
    Generate lab bloods file.
    """
    lab_swabs_description = lambda: {  # noqa: E731
        "Serum Source ID": _("random.custom_code", mask="ONS########", digit="#"),
        "Blood Sample Type": _("choice", items=["Venous", "Capillary"]),
        "Plate Barcode": _("random.custom_code", mask="ONS_######CS", digit="#"),
        "Well ID": _("random.custom_code", mask="@##", char="@", digit="#"),
        "Detection": _("choice", items=["DETECTION", "NOT detected", "failed"]),
        "Monoclonal Quantitation (Colourmetric)": _("float_number", start=0.0, end=3251.11, precision=4),
        "Monoclonal Bounded Quantitation(Colourmetric)": _("float_number", start=20, end=400, precision=1),
        "Monoclonal undiluted Quantitation(Colourmetric)": _("integer_number", start=0, end=20000),
        "Date ELISA Result record created": _("datetime.formatted_datetime", fmt="%Y-%m-%d", start=2018, end=2022),
        "Date Samples Arrayed Oxford": _("datetime.formatted_datetime", fmt="%Y-%m-%d", start=2018, end=2022),
        "Date Samples Received Oxford": _("datetime.formatted_datetime", fmt="%Y-%m-%d", start=2018, end=2022),
        "Voyager Date Created": _("datetime.formatted_datetime", fmt="%Y-%m-%d %H:%M:%S UTC", start=2018, end=2022),
    }
    schema = Schema(schema=lab_swabs_description)
    pandas_df = pd.DataFrame(schema.create(iterations=5))
    spark_df = spark_session.createDataFrame(pandas_df)
    return spark_df


def test_transform_bloods_delta(bloods_dummy_df, data_regression):
    transformed_df = transform_bloods_delta(bloods_dummy_df).toPandas().to_dict()
    print(transformed_df)
    data_regression.check(transformed_df)
