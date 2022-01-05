from chispa import assert_df_equality
from pyspark.sql import functions as F

from cishouseholds.weights.pre_calibration import grouping_from_lookup


def test_grouping_from_lookup(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            # fmt: off
                ("E12000001", 1,    'male',     1,      2,  1,  None), # for swabs
                ("E12000007", 7,    'female',   2,      25, 4,  2),
                ("N99999999", 12,   None,       None,   70, 7,  5),
            # fmt: on
        ],
        schema="""
                region_code string,
                interim_region_code integer,
                sex string,
                interim_sex integer,
                age_at_visit integer,
                age_group_swab integer,
                age_group_antibodies integer
            """,
    )
    input_df = expected_df.drop("interim_region_code", "interim_sex", "age_group_swab", "age_group_antibodies")

    output_df = grouping_from_lookup(df=input_df)

    assert_df_equality(output_df, expected_df, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)
