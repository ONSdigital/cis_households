from cishouseholds.extract import read_csv_to_pyspark_df
from cishouseholds.pipeline.declare_ETL import add_ETL
from cishouseholds.pyspark_utils import convert_cerberus_schema_to_pyspark
from cishouseholds.pyspark_utils import get_or_create_spark_session


@add_ETL("a_test_ETL")
def a_test_ETL(path: str):

    schema = {
        "test": {"type": "integer", "min": 1, "max": 9999},
    }

    spark_session = get_or_create_spark_session()
    spark_schema = convert_cerberus_schema_to_pyspark(schema)

    header = ",".join(schema.keys())
    df = read_csv_to_pyspark_df(
        spark_session,
        path,
        header,
        spark_schema,
        timestampFormat="yyyy-MM-dd HH:mm:ss 'UTC'",
    )
    df.show()
    return df
