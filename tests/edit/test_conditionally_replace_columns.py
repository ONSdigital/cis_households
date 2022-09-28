import pyspark.sql.functions as F
from chispa import assert_df_equality

from cishouseholds.edit import conditionally_replace_columns


def test_conditionally_replace_columns(spark_session):
    schema = "id integer, swab_sample_barcode string, blood_sample_barcode string, swab_sample_barcode_combined string, blood_sample_barcode_combined string, version integer"
    input_df = spark_session.createDataFrame(
        data=[
            (1, "P", "P", "U", "U", 2),  # fails condition so will remain the same
            (2, "P", None, "P", "U", 3),  # Tests replacement of None value with U 'user assigned' value
            (3, "P", None, "U", None, 3),  # Tests replacement of P 'pre-assigned' value with U
            (4, None, None, "U", None, 3),  # Tests replacement of None value with U
            (5, "P", "P", "U", "P", 3),  # Tests replacement of P with P and P with U
        ],
        schema=schema,
    )

    expected_df = spark_session.createDataFrame(
        data=[
            (1, "P", "P", "U", "U", 2),  # fails condition so will remain the same
            (2, "P", "U", "P", "U", 3),  # Tests replacement of None value with U 'user assigned' value
            (3, "U", None, "U", None, 3),  # Tests replacement of P 'pre-assigned' value with U
            (4, "U", None, "U", None, 3),  # Tests replacement of None value with U
            (5, "U", "P", "U", "P", 3),  # Tests replacement of P with P and P with U
        ],
        schema=schema,
    )

    output_df = conditionally_replace_columns(
        input_df,
        {
            "swab_sample_barcode": "swab_sample_barcode_combined",
            "blood_sample_barcode": "blood_sample_barcode_combined",
        },
        (F.col("version") == 3),
    )
    assert_df_equality(expected_df, output_df, ignore_nullable=True)
