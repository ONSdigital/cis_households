import pyspark.sql.functions as F
from chispa import assert_df_equality
from pyspark.sql.types import ArrayType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType

from cishouseholds.validate_class import SparkValidate


def test_sparkvalidate(spark_session):
    df_expected = spark_session.createDataFrame(
        data=[
            # fmt: off
                ('a',   1,  4,  'yes',  []),
                ('b',   2,  8,  'no',   ['inst_in', 'not_between']),
                ('aa',  12, 9,  'no',   ['not_contained']),
                ('ab',	8,  10, 'yes',  ['inst_in', 'not_between']),
                ('ab',	3,  10, 'yes',  ['inst_in', 'not_between']),
            # fmt: on
        ],
        schema=StructType(
            [
                StructField("column_1", StringType(), True),
                StructField("column_2", IntegerType(), True),
                StructField("column_3", IntegerType(), True),
                StructField("column_4", StringType(), True),
                StructField("error", ArrayType(StringType()), True),
            ]
        ),
    )
    df_input = df_expected.drop("error")

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

    def function_add_up_to(column_1, column_2):
        return F.col(column_1) + F.col(column_2) < 10

    validate_df.new_function("test_function", function_add_up_to, error_message="larger_than_10")

    # duplicate
    operations = {
        "duplicated": {"column_list": ["column_1", "column_2"]},
        "test_function": {"column_1": "column_2", "column_2": "column_3"},
    }
    validate_df.validate(operations=operations)

    # import pdb

    # pdb.set_trace()

    assert_df_equality(
        validate_df.dataframe, df_expected, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True
    )
