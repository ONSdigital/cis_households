from chispa import assert_df_equality
from pyspark.sql import functions as F

from cishouseholds.validate_class import SparkValidate


def test_sparkvalidate(spark_session):

    df_input = spark_session.createDataFrame(
        data=[
            # fmt: off
                ('a',   1,  7,  'yes'),
                ('b',   2,  8,  'no'),
                ('aa',  12, 9,  'no'),
                ('ab',	3,  10, 'yes'),
                ('ab',	3,  10, 'yes'),
            # fmt: on
        ],
        schema="""
                column_1 string,
                column_2 integer,
                column_3 integer,
                column_4 string
        """,
    )

<<<<<<< HEAD
    import pdb

    pdb.set_trace()
=======
>>>>>>> ef14753524871bb2a531105673fb90c1b271312e

    # initialise
    validate_df = SparkValidate(df_input)

    # validate
<<<<<<< HEAD
    operations = {"column_1": {"contains": "a"}}
=======
    operations = {"column_1": {'contains': 'a'}}

    import pdb; pdb.set_trace()
>>>>>>> ef14753524871bb2a531105673fb90c1b271312e
    validate_df.validate(operations=operations)

    # validate_df.dataframe.show()

    # execute_check

    # contains

    # inin
