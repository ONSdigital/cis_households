from cishouseholds.pyspark_utils import get_or_create_spark_session
from cishouseholds.extract import read_csv_to_pyspark_df
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def load_auxillary_data():
    spark_session = get_or_create_spark_session()
    resource_paths = {}
    auxillary_dfs = {}
    for name, resource_path in resource_paths.items():
        auxillary_dfs[name] = read_csv_to_pyspark_df(spark_session, resource_path,)

def update_data(old_sample_df: DataFrame, new_sample_df: DataFrame):
