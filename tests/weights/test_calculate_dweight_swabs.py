import pyspark.sql.functions as F
from chispa import assert_df_equality

from cishouseholds.weights.design_weights import calculate_raw_design_weight_swabs


def test_calculate_design_weight_swabs(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            ("A", "new", "A", 1, 2),
            ("B", "new", "A", 2, 2),
            ("C", "previous", "B", 3, 2),
            ("D", "previous", "B", 4, 2),
        ],
        schema="""
            cis_area_code_20 string,
            sample_type string,
            groupby string,
            id integer,
            previous_weight integer
            """,
    )
    household_df = spark_session.createDataFrame(
        data=[
            (1, 2, "C"),
            (2, 1, "B"),
            (1, 1, "A"),
        ],
        schema="""
            number_of_households_by_cis_area integer,
            number_of_households_by_country integer,
            cis_area_code_20 string
            """,
    )
    expected_df = spark_session.createDataFrame(
        data=[
            ("D", "previous", "B", 4, 2, None, None, 2, 2.0),
            ("C", "previous", "B", 3, 2, 1, 2, 2, 2.0),
            ("B", "new", "A", 2, 2, 2, 1, 2, 1.0),
            ("A", "new", "A", 1, 2, 1, 1, 2, 0.5),
        ],
        schema="""
            cis_area_code_20 string,
            sample_type string,
            groupby string,
            id integer,
            previous_weight integer,
            number_of_households_by_cis_area integer,
            number_of_households_by_country integer,
            number_eligible_household_sample integer,
            weight double
            """,
    )
    output_df = calculate_raw_design_weight_swabs(
        df=input_df,
        household_level_populations_df=household_df,
        column_name_to_assign="weight",
        sample_type_column="sample_type",
        group_by_columns=["groupby"],
        barcode_column="id",
        previous_design_weight_column="previous_weight",
    )

    assert_df_equality(output_df, expected_df, ignore_column_order=True, ignore_row_order=True, ignore_nullable=True)
