import pytest

from cishouseholds.extract import InvalidFileError
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
    expected_header = '"field_1"|"field_2"|"field_3"\n1|1|1'
    text_rdd = spark_session.sparkContext.parallelize([expected_header])
    assert validate_csv_header(text_rdd, expected_header=expected_header)


def test_validate_csv_header_with_invalid_header():
    spark_session = get_or_create_spark_session()
    expected_header = '"field_1"|"field_2"|"field_3"'
    text_rdd = spark_session.sparkContext.parallelize(['"blah"\n1'])
    assert not validate_csv_header(text_rdd, expected_header=expected_header)


# def test_validate_csv_header_error(gen_tmp_file):
#     test_text = '"field_1"|"field,2"|"field_3"'
#     test_text_error = '"field_1"|"field_2"|"field_3"'
#     test_file_path = gen_tmp_file("test_file.csv", test_text)
#     with pytest.raises(InvalidFileError):
#         validate_csv_header("file://" + test_file_path.as_posix(), expected_header=test_text_error)


@pytest.mark.skip("Doesn't run on HDFS")
def test_validate_csv_fields(gen_tmp_file):
    test_text = '"field_1"|"field,2"|"field_3"\n"entry_1"|"entry_2"|"entry_3"\n"entry_4"|"entry_5"|"entry_6"'
    test_file_path = gen_tmp_file("test_file.csv", test_text)
    assert validate_csv_fields(test_file_path.as_posix(), delimiter="|")


@pytest.mark.skip("Doesn't run on HDFS")
def test_validate_csv_fields_error(gen_tmp_file):
    test_text_error = (
        '"field_1"|"field,2"|"field_3"\n"entry_1"|"entry_2"|"entry_3"|"entry_3b"\n'
        '"entry_4"|"entry_5"|"entry_6"\n"entry_7"|"entry_8"'
    )
    test_file_path = gen_tmp_file("test_file.csv", test_text_error)
    with pytest.raises(InvalidFileError, match="1, 3"):
        validate_csv_fields(test_file_path.as_posix(), delimiter="|")
