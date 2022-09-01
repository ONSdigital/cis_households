from chispa import assert_df_equality
from pyspark.sql import functions as F

from cishouseholds.pipeline.reporting import multiple_visit_1_day


def test_multiple_visit_1_day(spark_session):
    schema = """
        participant_id integer,
        visit_id integer,
        date string,
        datetime string,
        repeated_last_visit integer
            """
    df = spark_session.createDataFrame(
        data=[
            # fmt: off
            (1,     1,      '2020-01-01',   '2020-01-01 00:00:00',      None),
            (1,     2,      '2020-01-01',   '2020-01-01 06:00:00',      1),

            (2,     3,      '2020-01-01',   '2020-01-01 00:00:00',      None),
            (2,     4,      '2020-01-02',   '2020-01-02 00:00:00',      None),
            # fmt: on
        ],
        schema=schema,
    )
    df_input = df.drop("repeated_last_visit")
    df_expected = df.filter(F.col("repeated_last_visit") == 1).drop("repeated_last_visit")

    df_output = multiple_visit_1_day(
        df=df_input,
        participant_id="participant_id",
        visit_id="visit_id",
        date_column="date",
        datetime_column="datetime",
    )
    assert_df_equality(df_output, df_expected, ignore_column_order=True, ignore_row_order=True, ignore_nullable=True)
