from chispa import assert_df_equality
from pyspark.sql import functions as F

from cishouseholds.derive import assign_nth_dose


def test_assign_nth_dose(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[("1", "2020-02-06", "No"), ("1", "2019-12-12", "Yes")],
        schema="""participant_id_column string, visit_datetime string, first_dose string""",
    )
    output_df = assign_nth_dose(
        df=expected_df.drop("first_dose"),
        column_name_to_assign="first_dose",
        participant_id_column="participant_id_column",
        visit_datetime="visit_datetime",
    )
    assert_df_equality(output_df, expected_df, ignore_nullable=True, ignore_row_order=True, ignore_column_order=True)
