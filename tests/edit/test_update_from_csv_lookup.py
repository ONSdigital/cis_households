# from chispa import assert_df_equality
from cishouseholds.edit import update_from_csv_lookup


def test_assign_ethnicity_white(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[(1, 1, "val1"), (2, 0, "val2"), (3, 1, "val3"), (3, 1, "val4")],
        schema="""id integer, A integer, B string""",
    )
    output_df = update_from_csv_lookup(expected_df, "C:/code/cis_households/cishouseholds/pipeline/Book1.csv")
    output_df = output_df
    # assert_df_equality(expected_df, output_df, ignore_nullable=True)
