import pytest
from chispa import assert_df_equality

from cishouseholds.derive import assign_consent_code


@pytest.mark.parametrize(
    "expected_data",
    [
        # v16, v5, v1, code
        (1, 0, 0, 16),
        (0, 1, 0, 5),
        (0, 0, 1, 1),
        (1, 1, 1, 16),
        (0, 0, 0, 0),
        (None, 0, 0, 0),
        (None, None, None, None),
    ],
)
def test_assign_consent_code(spark_session, expected_data):
    schema = "consent_v16 integer, consent_v5 integer, consent_v1 integer, consent_code integer"
    expected_df = spark_session.createDataFrame(data=[expected_data], schema=schema)

    input_df = expected_df.drop("consent_code")

    actual_df = assign_consent_code(input_df, "consent_code", ["consent_v16", "consent_v5", "consent_v1"])

    assert_df_equality(actual_df, expected_df)
