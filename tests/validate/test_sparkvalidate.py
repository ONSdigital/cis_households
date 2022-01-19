from chispa import assert_df_equality
from pyspark.sql import functions as F

from cishouseholds.validate_class import SparkValidate


def test_sparkvalidate(spark_session):

    df_input = spark_session.createDataFrame(
        data=[
            # fmt: off
                ('a',   1,  4,  'yes',  []),
                ('b',   2,  8,  'no',   ['inst_in', 'not_between']),
                ('aa',  12, 9,  'no',   ['not_contained']),
                ('ab',	8,  10, 'yes',  ['inst_in', 'not_between']),
                ('ab',	3,  10, 'yes',  ['inst_in', 'not_between']),
            # fmt: on
        ],
        schema="""
                column_1 string,
                column_2 integer,
                column_3 integer,
                column_4 string,
                error array
        """,
    )

    # initialise
    validate_df = SparkValidate(df_input)

    validation_checks_dict = {
        "column_1": {"contains": "a"},
        "column_4": {"isin": "no"},
        "column_3": {
            "between": {"lower_bound": {"inclusive": True, "value": 8}, "upper_bound": {"inclusive": True, "value": 9}}
        },
    }

    validate_df.validate_column(operations=validation_checks_dict)

    # duplicate
    operations = {"duplicated": ["column_1", "column_2"]}
    validate_df.validate(operations=operations)
