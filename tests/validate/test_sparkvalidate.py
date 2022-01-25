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
                ('a',   1,  4,  'yes', ["column_4, the row is 'no'", 'column_3 should be in between lower_bound and upper_bound.','col_2 and col_3 should_be_within_interval 4 and 10']),
                ('aa',  12, 9,  'no', ['larger_than_10']),
                ('ab',	3,  10, 'yes', ["column_4, the row is 'no'", 'column_3 should be in between lower_bound and upper_bound.','col_2 and col_3 should_be_within_interval 4 and 10', 'column_1, column_3 should be unique.', 'larger_than_10']),
                ('ab',	8,  10, 'yes', ["column_4, the row is 'no'", 'column_3 should be in between lower_bound and upper_bound.','col_2 and col_3 should_be_within_interval 4 and 10', 'column_1, column_3 should be unique.', 'larger_than_10']),
                ('b',   2,  7,  'no', ["column_1 should contain 'a'.",'column_3 should be in between lower_bound and upper_bound.', 'col_2 and col_3 should_be_within_interval 4 and 10']),
                (None,   2,  7,  'no',   ['column_3 should be in between lower_bound and upper_bound.', 'col_2 and col_3 should_be_within_interval 4 and 10']),
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

    # initialise dataframe
    validate_df = SparkValidate(df_input, "error")

    # single column test
    validation_checks_dict = {
        "column_1": {"contains": "a"},
        "column_4": {"isin": ["no"]},
        "column_3": {
            "between": {"lower_bound": {"inclusive": True, "value": 8}, "upper_bound": {"inclusive": True, "value": 9}}
        },
    }
    validate_df.validate_column(operations=validation_checks_dict)

    # user defined function external definition
    def function_add_up_to(error_message, column_1, column_2):
        return (F.col(column_1) + F.col(column_2)) < 10, error_message

    validate_df.new_function("test_function", function_add_up_to, error_message="larger_than_10")

    # user defined function directly
    validate_df.validate_udl(
        logic=((F.col("column_2") > 4) & (F.col("column_3") < 10)),
        error_message="col_2 and col_3 should_be_within_interval 4 and 10",
    )
    # duplicate
    operations = {
        "duplicated": {"check_columns": ["column_1", "column_3"]},
        "test_function": {"column_1": "column_2", "column_2": "column_3"},
    }
    validate_df.validate(operations=operations)

    validate_df.dataframe.toPandas().to_csv("out.csv", index=False)

    assert_df_equality(
        validate_df.dataframe, df_expected, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True
    )
