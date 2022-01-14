from chispa import assert_df_equality

from cishouseholds.filter import file_exclude


def test_file_exclude(spark_session):  # test funtion
    input_data = [
        ("ONS10001", "10152021.csv"),  # to drop
        ("ONS10002", "22452020.csv"),  # to drop
        ("ONS10003", "10152021.csv"),  # to drop
        ("ONS10004", "31022020.csv"),
        ("ONS10005", "19112019.csv"),
    ]

    expected_data = [("ONS10004", "31022020.csv"), ("ONS10005", "19112019.csv")]

    input_df = spark_session.createDataFrame(data=input_data, schema="ref_id string, file_path_column string")

    expected_df = spark_session.createDataFrame(data=expected_data, schema="ref_id string, file_path_column string")

    file_exclude_list = ["10152021.csv", "22452020.csv"]

    result_df = file_exclude(input_df, "file_path_column", file_exclude_list)

    assert_df_equality(result_df, expected_df, ignore_row_order=True, ignore_column_order=True)
