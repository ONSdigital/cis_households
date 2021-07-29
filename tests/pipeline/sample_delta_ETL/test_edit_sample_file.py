from chispa import assert_df_equality

from cishouseholds.pipeline.sample_delta_ETL import edit_sample_file


def test_edit_sample_file(spark_session):

    input_schema = "custodian_region_code string, laua string, rgn string"
    input_df = spark_session.createDataFrame(
        data=[
            ("S92000003", "S00001111", None),
            ("W92000004", "W00001111", "W92000004"),
            ("N99999999", "N00001111", "N92000001"),
            ("E12000002", "E07000120", None),
        ],
        schema=input_schema,
    )

    expected_schema = (
        "laua string, rgn string, sample string, sample_direct integer,"
        "gor9d string, country_sample string, gor9d_recoded string"
    )
    expected_df = spark_session.createDataFrame(
        data=[
            ("S00001111", "S99999999", "samp1", 1, "S99999999", "Scotland", "S99999999"),
            ("W00001111", "W92000004", "samp1", 1, "W99999999", "Wales", "W99999999"),
            ("N00001111", "N92000001", "samp1", 1, "N99999999", "NI", "N99999999"),
            ("E07000120", "E12000002", "samp1", 1, "E12000002", "England", "E12000002_boost"),
        ],
        schema=expected_schema,
    )

    output_df = edit_sample_file(input_df, "samp1", 1)

    assert_df_equality(output_df, expected_df, ignore_nullable=True)
