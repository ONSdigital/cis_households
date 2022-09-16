from email import header

from chispa import assert_df_equality

from cishouseholds.validate import normalise_schema


def test_normalise_scehma(spark_session, pandas_df_to_temporary_csv):
    """Test that validator works to assign failed flags to columns where result is not unique"""
    input_df1 = spark_session.createDataFrame(
        data=[
            ("ONS00000001", 1, None),
        ],
        schema="colA string, colB integer, colC integer",
    )
    input_df2 = spark_session.createDataFrame(
        data=[
            ("ONS00000001", 1, None),
        ],
        schema="blaA string, blaB integer, blaC integer",
    )
    input_df3 = spark_session.createDataFrame(
        data=[
            ("ONS00000001", 1, None),
        ],
        schema="bla1 string, bla2 integer, bla3 integer",
    )
    input_df4 = spark_session.createDataFrame(
        data=[
            ("blah", None, None, 1, None),
            ("blaA", "colB", "blaC", "add_1", "add_2"),
            ("ONS00000001", 1, None, 1, 1),
        ],
        schema="bla string, bla string, bla string, add_1 string, add_2 string",
    )

    expected_df = spark_session.createDataFrame(
        data=[
            ("ONS00000001", 1, None),
        ],
        schema="colA string, colB integer, colC integer",
    )

    schema = {
        "colA": {"type": "string"},
        "colB": {"type": "integer"},
        "colC": {"type": "integer"},
    }

    regex_schema = {r"A": "colA", r"B": "colB", r"C": "colC"}

    csv_path = pandas_df_to_temporary_csv(input_df4.toPandas(), ",", header=False)
    error, df = normalise_schema(csv_path, schema, regex_schema)
    assert_df_equality(df, expected_df)

    for df in [input_df1, input_df2, input_df3]:
        csv_path = pandas_df_to_temporary_csv(df.toPandas(), ",")
        error, df = normalise_schema(csv_path, schema, regex_schema)
        assert_df_equality(df, expected_df)
