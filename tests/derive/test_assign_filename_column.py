import pandas as pd
from chispa import assert_df_equality

from cishouseholds.derive import assign_filename_column
from cishouseholds.pipeline.input_file_processing import extract_input_data


def test_assign_filename_column(pandas_df_to_temporary_csv, spark_session):
    pandas_df = pd.DataFrame(
        data={
            "id": [0, 1],
            "dummy": ["first_value", "second_value"],
        }
    )
    csv_file_path = pandas_df_to_temporary_csv(pandas_df, sep="|")
    path = "file:///" + str(csv_file_path.as_posix()).lstrip("/")
    expected_df = spark_session.createDataFrame(
        data=[
            (0, "first_value", path),
            (1, "second_value", path),
        ],
        schema="id string, dummy string, csv_filename string",
    )
    input_df = extract_input_data(csv_file_path.as_posix(), None, sep="|")
    output_df = assign_filename_column(input_df, "csv_filename")
    assert_df_equality(expected_df, output_df, ignore_nullable=True)
