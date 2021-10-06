import pytest

from cishouseholds.pyspark_utils import get_or_create_spark_session
from cishouseholds.validate import validate_csv_fields
from cishouseholds.validate import validate_csv_header


@pytest.fixture
def gen_tmp_file(tmp_path):
    def _(file_name, file_content):
        test_file_path = tmp_path / file_name
        test_file_path.touch()
        test_file_path.write_text(file_content)
        return test_file_path

    return _


def test_validate_csv_header_with_valid_header():
    spark_session = get_or_create_spark_session()
    expected_header = '"field_1"|"field_2"|"field_3"'
    text_rdd = spark_session.sparkContext.parallelize(['"field_1"|"field_2"|"field_3"', "1|1|1"])
    assert validate_csv_header(text_rdd, expected_header=expected_header)


def test_validate_csv_header_with_invalid_header():
    spark_session = get_or_create_spark_session()
    expected_header = '"field_1"|"field_2"|"field_3"'
    text_rdd = spark_session.sparkContext.parallelize(['"blah"\n1'])
    assert not validate_csv_header(text_rdd, expected_header=expected_header)


def test_validate_csv_fields():
    spark_session = get_or_create_spark_session()
    text_rdd = spark_session.sparkContext.parallelize(['"field_1"|"field_2"|"field_3"', "1|1|1"])
    assert validate_csv_fields(text_rdd, delimiter="|")


def test_validate_csv_fields_error():
    spark_session = get_or_create_spark_session()
    text_rdd = spark_session.sparkContext.parallelize(['"field_1"|"field_2"|"field_3"', "1|1|1|1"])
    assert not validate_csv_fields(text_rdd, delimiter="|")
