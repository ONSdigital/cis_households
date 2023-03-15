import pyspark.sql.functions as F
from chispa import assert_df_equality

from cishouseholds.derive import assign_survey_not_completed_reason_code


def test_assign_survey_not_completed_reason_code(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            (None, None, "Do this questionnaire only", None, "FNR"),
            (None, None, "Do this questionnaire and take a swab sample", None, "FNR"),
            (None, 1, "Do this questionnaire and take a swab sample", None, "QNR"),
            (1, 1, "Do this questionnaire and take a swab sample and a blood sample", None, "QNR"),
            (
                1,
                1,
                "Do this questionnaire only",
                1,
                None,
            ),  # having done questionaire only and completed currently leads to nothing
            (1, None, "Do this questionnaire and take a swab sample", 1, "TNR"),
            (None, 1, "Do this questionnaire and take a swab sample and a blood sample", 1, "FNR"),
            (None, None, "Do this questionnaire and take a swab sample and a blood sample", 1, "TNR"),
        ],
        schema="blood integer, swab integer, cohort_type string, filled integer, result string",
    )
    output_df = assign_survey_not_completed_reason_code(
        df=expected_df.drop("result"),
        column_name_to_assign="result",
        swab_barcode_column="swab",
        blood_barcode_column="blood",
        cohort_type_column="cohort_type",
        survey_filled_column="filled",
    )
    assert_df_equality(output_df, expected_df, ignore_nullable=True, ignore_row_order=True)
