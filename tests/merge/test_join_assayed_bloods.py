import pyspark.sql.functions as F
from chispa.dataframe_comparer import assert_df_equality

from cishouseholds.merge import join_assayed_bloods


def test_join_assayed_bloods(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            ("S", "1", 1, "A", "A", "A"),  # singular
            ("N", "3", 1, "A", "A", "A"),  # match N and S
            ("N", "2", 1, "A", "A", "A"),  # singular
            ("S", "3", 1, "A", "A", "A"),
            ("N", "4", 1, "A", "A", "A"),  # duplicated N, so id should fail
            ("S", "4", 1, "A", "A", "A"),
            ("N", "4", 1, "A", "A", "A"),
            ("N", "5", 1, None, "B", "A"),
            ("S", "5", 1, None, "B", "A"),
            ("S", "6", 1, "A", "B", "A"),
            ("N", "6", 1, "C", "A", "A"),
        ],
        schema="""blood_group string, unique_antibody_test_id string, col1 integer, blood_sample_barcode string, antibody_test_plate_common_id string, antibody_test_well_id string""",
    )
    expected_df = spark_session.createDataFrame(
        data=[
            ("1", 1, None),
            ("2", None, 1),
            ("3", 1, 1),
            ("5", None, 1),
            ("5", 1, None),
            ("6", None, 1),
            ("6", 1, None),
        ],
        schema="""unique_antibody_test_id string,col1_s_protein integer,col1_n_protein integer""",
    )
    other_join_cols = ["blood_sample_barcode", "antibody_test_plate_common_id", "antibody_test_well_id"]

    expected_error_df = input_df.filter(F.col("unique_antibody_test_id") == "4")
    output_df, error_df = join_assayed_bloods(input_df, "blood_group", "unique_antibody_test_id")
    output_df = output_df.drop(*other_join_cols)

    assert_df_equality(expected_df, output_df, ignore_row_order=True)
    assert_df_equality(expected_error_df, error_df, ignore_row_order=True)
