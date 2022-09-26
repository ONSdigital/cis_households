import pyspark.sql.functions as F
from chispa import assert_df_equality

from cishouseholds.edit import conditionally_replace_columns


def test_conditionally_replace_columns(spark_session):
    schema = "swab_sample_barcode string, blood_sample_barcode string, swab_sample_barcode_combined string, blood_sample_barcode_combined string, version integer"
    input_df = spark_session.createDataFrame(
        data=[
            ("A", "B", "C", "D", 2),  # fails condition so will remain the same
            ("A", "B", "C", "D", 3),
            ("A", None, None, "D", 3),
            (None, None, "C", "B", 3),
            ("A", "B", None, "D", 3),
        ],
        schema=schema,
    )

    expected_df = spark_session.createDataFrame(
        data=[
            ("A", "B", "C", "D", 2),
            ("A", "D", "C", "D", 3),
            ("A", "D", None, "D", 3),
            (None, "B", "C", "B", 3),
            ("A", "D", None, "D", 3),
        ],
        schema=schema,
    )

    output_df = conditionally_replace_columns(
        input_df,
        {
            # "swab_sample_barcode": "swab_sample_barcode_combined",
            "blood_sample_barcode": "blood_sample_barcode_combined"
        },
        (F.col("version") == 3),
    )
    assert_df_equality(expected_df, output_df, ignore_nullable=True)
