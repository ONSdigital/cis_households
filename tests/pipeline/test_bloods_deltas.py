import pandas as pd
import pytest
from mimesis.schema import Field
from mimesis.schema import Schema

from cishouseholds.pipeline.bloods_delta_ETL import transform_bloods_delta

_ = Field("en-gb", seed=42)


@pytest.fixture
def input_df(spark_session):
    """
    Generate lab bloods file.
    """
    lab_swabs_description = lambda: {  # noqa: E731
        "plate_tdi": _("random.generate_string", str_seq="a1b2c3d4e5f6g7h8", length=12),  # substring 5 chars 5-10
        "result_tdi": _("choice", items=["Positive", "Negative", None]),
        "result_siemens": _("choice", items=["Positive", "Negative"]),
    }
    schema = Schema(schema=lab_swabs_description)
    pandas_df = pd.DataFrame(schema.create(iterations=5))
    spark_df = spark_session.createDataFrame(pandas_df)
    spark_df.show()
    return spark_df


def test_transform_bloods_delta(input_df, data_regression):
    transformed_df = transform_bloods_delta(input_df).toPandas().to_dict()
    print(transformed_df)
    data_regression.check(transformed_df)
