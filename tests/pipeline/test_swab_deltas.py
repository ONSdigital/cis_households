import pytest
from mimesis.schema import Field, Schema
import pandas as pd

_ = Field('en-gb', seed=42)

@pytest.fixture
def input_df(spark_session):
    """
    Generate lab swabs file.
    """
    lab_swabs_description = (
        lambda: {
            'Sample':_('random.custom_code', mask='ONS########', digit='#'),
            'result_mk_date_time': _('datetime.formatted_datetime', fmt="%Y-%m-%d %H:%M:%S UTC", start=1800, end=1802),
            'ctORF1ab': _('float_number', start=10.0, end=40.0, precision=2),
            'ctNgene': _('float_number', start=10.0, end=40.0, precision=2),
            'ctSgene': _('float_number', start=10.0, end=40.0, precision=2),
        }
    )

    schema = Schema(schema=lab_swabs_description)
    pandas_df = pd.DataFrame(schema.create(iterations=5))
    spark_df = spark_session.createDataFrame(pandas_df)
    spark_df.show()
    return spark_df