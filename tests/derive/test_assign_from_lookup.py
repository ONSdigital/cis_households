import pytest
from chispa import assert_df_equality

from cishouseholds.derive import assign_from_lookup


@pytest.fixture
def expected_df(spark_session):
    return spark_session.createDataFrame(
        data=[
            ("No", "No", 0),
            ("Yes", "No", 3),
            ("Yes", "Yes", 1),
            ("Yes", None, 2),
            ("No", "Yes", 4),
            (None, "Yes", None),
            ("No", None, None),
            (None, None, None),
        ],
        schema=["reference_1", "reference_2", "outcome"],
    )


@pytest.fixture
def lookup_df(spark_session):
    return spark_session.createDataFrame(
        data=[
            ("No", "No", 0),
            ("Yes", "Yes", 1),
            ("Yes", None, 2),
            ("Yes", "No", 3),
            ("No", "Yes", 4),
        ],
        schema=["reference_1", "reference_2", "outcome"],
    )


def test_assign_from_lookup(spark_session, expected_df, lookup_df):

    input_df = expected_df.drop("outcome")

    actual_df = assign_from_lookup(input_df, "outcome", ["reference_1", "reference_2"], lookup_df)

    assert_df_equality(actual_df, expected_df)


def test_assign_from_lookup_schema_error_lookup(spark_session, expected_df, lookup_df):

    input_df = expected_df.drop("outcome")

    lookup_df = lookup_df.withColumnRenamed("reference_2", "reference_3")

    with pytest.raises(ValueError, match="reference_2"):
        assign_from_lookup(input_df, "outcome", ["reference_1", "reference_2"], lookup_df)


def test_assign_from_lookup_schema_error_df(spark_session, expected_df, lookup_df):

    input_df = expected_df.drop("outcome")

    input_df = input_df.withColumnRenamed("reference_2", "reference_3")

    with pytest.raises(ValueError, match="reference_2"):
        assign_from_lookup(input_df, "outcome", ["reference_1", "reference_2"], lookup_df)


def test_assign_from_lookup_column_to_assign_error(spark_session, expected_df, lookup_df):

    input_df = expected_df.drop("outcome")

    with pytest.raises(ValueError, match="bad_name"):
        assign_from_lookup(input_df, "bad_name", ["reference_1", "reference_2"], lookup_df)
