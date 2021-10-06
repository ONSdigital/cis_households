import pandas as pd
import pytest
from mimesis.schema import Field
from mimesis.schema import Schema

from cishouseholds.edit import rename_column_names
from cishouseholds.pipeline.bloods_delta_ETL import bloods_delta_ETL
from cishouseholds.pipeline.bloods_delta_ETL import transform_bloods_delta
from cishouseholds.pipeline.input_variable_names import bloods_variable_name_map

_ = Field("en-gb", seed=42)


@pytest.fixture
def bloods_dummy_df(spark_session):
    """
    Generate lab bloods file.
    """
    lab_bloods_description = lambda: {  # noqa: E731
        "Serum Source ID": _("random.custom_code", mask="ONS########", digit="#"),
        "Blood Sample Type": _("choice", items=["Venous", "Capillary"]),
        "Plate Barcode": _("random.custom_code", mask=f"ONS_######C{target}-#", digit="#"),
        "Well ID": _("random.custom_code", mask="@##", char="@", digit="#"),
        "Detection": _("choice", items=["DETECTED", "NOT detected", "failed"]),
        "Monoclonal quantitation (Colourimetric)": _("float_number", start=0.0, end=3251.11, precision=4),
        "Monoclonal bounded quantitation (Colourimetric)": _("float_number", start=20, end=400, precision=1),
        "Monoclonal undiluted quantitation (Colourimetric)": _("integer_number", start=0, end=20000),
        "Date ELISA Result record created": _("datetime.formatted_datetime", fmt="%Y-%m-%d", start=2018, end=2022),
        "Date Samples Arrayed Oxford": _("datetime.formatted_datetime", fmt="%Y-%m-%d", start=2018, end=2022),
        "Date Samples Received Oxford": _("datetime.formatted_datetime", fmt="%Y-%m-%d", start=2018, end=2022),
        "Voyager Date Created": _("datetime.formatted_datetime", fmt="%Y-%m-%d %H:%M:%S", start=2018, end=2022),
    }
    schema = Schema(schema=lab_bloods_description)
    pandas_df = pd.DataFrame(schema.create(iterations=5))
    return pandas_df


def test_transform_bloods_delta(bloods_dummy_df, spark_session, data_regression):

    bloods_dummy_df = spark_session.createDataFrame(bloods_dummy_df)
    bloods_dummy_df = rename_column_names(bloods_dummy_df, bloods_variable_name_map)
    transformed_df = transform_bloods_delta(bloods_dummy_df).toPandas().to_dict()
    data_regression.check(transformed_df)


def test_bloods_delta_ETL_end_to_end(bloods_dummy_df, pandas_df_to_temporary_csv):
    """
    Test that valid example data flows through the ETL from a csv file.
    """
    csv_file = pandas_df_to_temporary_csv(bloods_dummy_df)
    bloods_delta_ETL(csv_file.as_posix())
