import pyspark.sql.functions as F
from chispa.dataframe_comparer import assert_df_equality

from cishouseholds.merge import join_assayed_bloods


def test_join_assayed_bloods(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            ("S", "1", 1),  # singular
            ("N", "2", 1),  # singular
            ("N", "3", 1),  # match N and S
            ("S", "3", 1),
            ("N", "4", 1),  # duplicated N, so id should fail
            ("S", "4", 1),
            ("N", "4", 1),
        ],
        schema="""blood_group string, unique_antibody_test_id string, col1 integer""",
    )
    expected_df = spark_session.createDataFrame(
        data=[
            ("1", 1, None),
            ("2", None, 1),
            ("3", 1, 1),
        ],
        schema="""unique_antibody_test_id string,col1_s_protein integer,col1_n_protein integer""",
    )
    other_join_cols = ["blood_sample_barcode", "antibody_test_plate_common_id", "antibody_test_well_id"]
    for col in other_join_cols:
        input_df = input_df.withColumn(col, F.lit("a"))

    expected_error_df = input_df.filter(F.col("unique_antibody_test_id") == "4")
    output_df, error_df = join_assayed_bloods(input_df, "blood_group")
    output_df = output_df.drop(*other_join_cols)

    assert_df_equality(expected_df, output_df, ignore_row_order=True)
    assert_df_equality(expected_error_df, error_df, ignore_row_order=True)
