import pandas as pd
import pytest
from mimesis.schema import Field
from mimesis.schema import Schema

from cishouseholds.pipeline.swab_delta_ETL import transform_swabs_delta

_ = Field("en-gb", seed=42)


@pytest.fixture
def input_df(spark_session):
    """
    Generate lab swabs file.
    """
    lab_swabs_description = lambda: {  # noqa: E731
        "Sample": _("random.custom_code", mask="ONS########", digit="#"),
        "result_mk_date_time": _("datetime.formatted_datetime", fmt="%Y-%m-%d %H:%M:%S UTC", start=1800, end=1802),
        "ctORF1ab": _("float_number", start=10.0, end=40.0, precision=2),
        "ctNgene": _("float_number", start=10.0, end=40.0, precision=2),
        "ctSgene": _("float_number", start=10.0, end=40.0, precision=2),
    }
    schema = Schema(schema=lab_swabs_description)
    pandas_df = pd.DataFrame(schema.create(iterations=5))
    spark_df = spark_session.createDataFrame(pandas_df)
    spark_df.show()
    return spark_df


def test_transform_swabs_delta_ETL(input_df, spark_session, data_regression):
    transformed_df = transform_swabs_delta(input_df, spark_session).toPandas().to_dict()
    print(transformed_df)
    data_regression.check(transformed_df)
