# Create an example
import pytest

@pytest.fixture
def expected_df(spark_session):
    return spark_session.createDataFrame(
        data = [
            (1, 0, 1) # this value should convert to (0, 0, 1)
        ],
        schema = ["ctNgene_result","ctNgene","result_mk"]
    )

# Assert