from pathlib import Path

import pyspark.sql.functions as F
from chispa import assert_df_equality

from cishouseholds.weights.extract import read_csv_to_pyspark_df
from cishouseholds.weights.population_projections import proccess_population_projection_df


def test_end_to_end_population_projection(spark_session):
    current_population_df = spark_session.createDataFrame(
        schema="""
            laua string,
            rgn string,
            ctry string,
            ctry_name string,
            surge integer,
            m0 double,
            m25 double,
            f0 double,
            f25 double
        """,
        data=[
            ("N09000003", "N99999999", "N92000002", "Northern Ireland", None, 334.0, 346.0, None, 604.5213708),
            ("N09000003", "N99999999", "N92000002", "Northern Ireland", None, 468.0, 449.0, None, 88.12197785),
            ("N09000004", "N99999999", "N92000002", "Northern Ireland", None, 513.0, 439.0, None, 530.2306306),
            ("E06000001", "E12000001", "E92000001", "England", None, 576.0, 512.0, None, 126.6188443),
            ("E06000002", "E12000001", "E92000001", "England", None, 984.9199157, 1006.0, None, 493.6140762),
            ("E06000003", "E12000001", "E92000001", "England", None, 726.0, 755.0, None, 1530.113916),
            ("W06000001", "W99999999", "W92000004", "Wales", None, 349.0, 333.0, None, 396.5347087),
            ("W06000002", "W99999999", "W92000004", "Wales", None, 612.360515, 612.0, None, 1196.549768),
            ("W06000003", "W99999999", "W92000004", "Wales", None, 552.0107335, 573.0465116, None, 1406.389562),
            ("W06000004", "W99999999", "W92000004", "Wales", None, 508.0694698, 537.0, None, 351.10133),
            ("W06000005", "W99999999", "W92000004", "Wales", None, 833.0, 827.0, None, 2480.652093),
            ("S12000008", "S99999999", "S92000003", "Scotland", None, 671.8524403, 697.390691, None, 862.4606535),
            ("S12000010", "S99999999", "S92000003", "Scotland", None, 558.829564, 522.6814978, None, 484.3186683),
            ("S12000011", "S99999999", "S92000003", "Scotland", None, 453.2125183, 509.115384, None, 949.0701877),
            ("S12000013", "S99999999", "S92000003", "Scotland", None, 123.0232389, 106.1571497, None, 493.4904287),
        ],
    )
    previous_population_df = spark_session.createDataFrame(
        schema="""
            laua string,
            rgn string,
            ctry string,
            ctry_name string,
            surge integer,
            m0 double,
            m25 double,
            f0 double,
            f25 double
        """,
        data=[
            ("N09000003", "N99999999", "N92000002", "Northern Ireland", None, 364.0, 387.0, None, 606.0),
            ("N09000003", "N99999999", "N92000002", "Northern Ireland", None, 533.0, 478.0, None, 81.0),
            ("N09000004", "N99999999", "N92000002", "Northern Ireland", None, 546.0, 444.0, None, 532.0),
            ("E06000001", "E12000001", "E92000001", "England", None, 599.0, 501.0, None, 135.0),
            ("E06000002", "E12000001", "E92000001", "England", None, 847.0, 1093.0, None, 500.0),
            ("E06000003", "E12000001", "E92000001", "England", None, 738.0, 789.0, None, 167.0),
            ("W06000001", "W99999999", "W92000004", "Wales", None, 354.0, 367.0, None, 300.0),
            ("W06000002", "W99999999", "W92000004", "Wales", None, 654.0, 634.0, None, 1125.0),
            ("W06000003", "W99999999", "W92000004", "Wales", None, 553.0, 588.0, None, 1465.0),
            ("W06000004", "W99999999", "W92000004", "Wales", None, 786.0, 555.0, None, 342.0),
            ("W06000005", "W99999999", "W92000004", "Wales", None, 243.0, 321.0, None, 2890.0),
            ("S12000008", "S99999999", "S92000003", "Scotland", None, 666.0, 635.927, None, 8678.0),
            ("S12000010", "S99999999", "S92000003", "Scotland", None, 345.0, 522.194, None, 498.0),
            ("S12000011", "S99999999", "S92000003", "Scotland", None, 476.0, 588.65, None, 990.0),
            ("S12000013", "S99999999", "S92000003", "Scotland", None, 192.0, 105.11, None, 438.49),
        ],
    )
    aps_lookup = spark_session.createDataFrame(
        schema="""
            CASENO integer,
            COUNTRY integer,
            AGE integer,
            ETHGBEUL string,
            ETH11NI string,
            PWTA18 double
        """,
        data=[
            (14, 2, 46, 11, "NA", 9667.496677),
            (15, 1, 66, 8, "NA", 25871.01269),
            (16, 5, 29, "NA", 6, 17152.30926),
            (17, 1, 82, 9, "NA", 23203.82533),
            (18, 1, 57, 1, "NA", 25404.24882),
            (19, 4, 65, 1, "NA", 6139.407165),
            (20, 1, 58, 8, "NA", 28241.52192),
        ],
    )

    auxillary_dfs = {
        "population_projection_current": current_population_df,
        "population_projection_previous": previous_population_df,
        "aps_lookup": aps_lookup,
    }

    expected_df_path = "tests/weights/test_files/output2.csv"
    expected_df = spark_session.read.csv(
        expected_df_path, header=True, schema=None, ignoreLeadingWhiteSpace=True, ignoreTrailingWhiteSpace=True, sep=","
    )

    output_df = proccess_population_projection_df(dfs=auxillary_dfs, month=7)

    for col, type in output_df.dtypes:
        expected_df = expected_df.withColumn(col, F.col(col).cast(type))

    assert_df_equality(output_df, expected_df, ignore_column_order=True, ignore_row_order=True, ignore_nullable=True)
