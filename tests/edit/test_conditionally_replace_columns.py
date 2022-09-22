import pyspark.sql.functions as F
from chispa import assert_df_equality

from cishouseholds.edit import conditionally_replace_columns


def test_conditionally_replace_columns(spark_session):
    schema = "swab_barcode string, blood_barcode string, swab_barcode_combined string, blood_barcode_combined string, version integer"
    input_df = spark_session.createDataFrame(
        data=[
            ("A", "B", "C", "D", 2),  # fails condition so will remain the same
            ("A", None, "C", "D", 3),
            ("A", None, None, "D", 3),
            (None, None, "C", "D", 3),
            ("A", "B", None, None, 3),
        ],
        schema=schema,
    )

    expected_df = spark_session.createDataFrame(
        data=[
            ("A", "B", "C", "D", 2),
            ("C", "D", "C", "D", 3),
            (None, "D", None, "D", 3),
            ("C", "D", "C", "D", 3),
            (None, None, None, None, 3),
        ],
        schema=schema,
    )

    output_df = conditionally_replace_columns(
        input_df,
        {"swab_barcode": "swab_barcode_combined", "blood_barcode": "blood_barcode_combined"},
        (F.col("version") == 3),
    )
    assert_df_equality(expected_df, output_df, ignore_nullable=True)
