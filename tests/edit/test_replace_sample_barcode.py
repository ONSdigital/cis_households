from chispa import assert_df_equality

from cishouseholds.edit import replace_sample_barcode


def test_replace_sample_barcode(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            ("A", "B", "C", "D"),
            ("A", None, "C", "D"),
            ("A", None, None, "D"),
            (None, None, "C", "D"),
            ("A", "B", None, None),
        ],
        schema="""swab_barcode_combined string, blood_barcode_combined string, swab_barcode string, blood_barcode string""",
    )

    expected_df = spark_session.createDataFrame(
        data=[
            ("A", "B", "A", "B"),
            ("A", None, "A", "D"),
            ("A", None, "A", "D"),
            (None, None, "C", "D"),
            ("A", "B", "A", "B"),
        ],
        schema="""swab_barcode_combined string, blood_barcode_combined string, swab_barcode string, blood_barcode string""",
    )

    output_df = replace_sample_barcode(
        input_df, "swab_barcode_combined", "blood_barcode_combined", "swab_barcode", "blood_barcode"
    )
    assert_df_equality(expected_df, output_df, ignore_nullable=True)


"""

The above should happen after barcode cleaning, and after the creation of {}_sample_barcode_combined in high_level_transformations.


Shoehorning in an addition to this one, to account for iqvia editing of the barcode_user_entered field
(when the user actually uses their pre-assigned barcode in the user entered field), but the field barcode_correct still being 'No'.

Adapt high_level_transformations.py line 2075 to add in the following condition:

If swab_sample_barcode_correct = 'Yes' and swab_sample_barcode_user_entered is null, then take user swab_sample_barcode.
"""


"""
 if "swab_sample_barcode_user_entered" in df.columns:
            df = df.withColumn(
                f"swab_sample_barcode_combined", F.when(F.col(f"swab_sample_barcode_correct") == "No", F.col(f"swab_sample_barcode_user_entered"),)
                                                .otherwise(F.col(f"swab_sample_barcode"))
                # set to sample_barcode if _sample_barcode_correct is yes or null.
            )
"""
