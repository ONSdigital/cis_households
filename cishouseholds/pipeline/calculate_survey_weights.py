import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.window import Window

from cishouseholds.pipeline.sample_delta_ETL import calculate_design_weights
from cishouseholds.pipeline.sample_delta_ETL import edit_sample_file
from cishouseholds.pipeline.sample_delta_ETL import extract_from_csv
from cishouseholds.pyspark_utils import get_or_create_spark_session

spark_session = get_or_create_spark_session()


def calculate_survey_weights():
    calculate_design_weights()
    calibrate_survey_weights()
    load_weights_to_database()


def get_matching_columns(df1: DataFrame, df2: DataFrame):
    df1_schema = df1.schema.fields
    df2_schema = df2.schema.fields
    df1_names = [x.name for x in df1_schema]
    df2_names = [x.name for x in df2_schema]
    matches = set(df1_names) & set(df2_names)
    df_1_missmatches = set(df1_names) ^ matches
    df_2_missmatches = set(df2_names) ^ matches
    return list(df_1_missmatches), list(df_2_missmatches)


def combine_design_weights(new_sample_name: str):
    dataframes = extract_from_csv()
    new_sample_df = edit_sample_file(dataframes["sample_direct"], new_sample_name, 1)
    household_population_df = dataframes["household"]
    previous_sample_df = dataframes["previous"]
    lookup_df = dataframes["lookup"]

    weighted_new_df = calculate_design_weights(new_sample_df, household_population_df)

    def calculate_sample_size(df: DataFrame):
        dft = (
            df.groupBy("interim_id")
            .count()
            .withColumnRenamed("interim_id", "i_id")
            .withColumnRenamed("count", "sample_size")
        )
        return df.join(dft, dft.i_id == df.interim_id)

    def update_interim_id(df_old: DataFrame, df_new: DataFrame, lookup_df: DataFrame):
        """
        join previous (old) df and new dample (new) df onto lookup for interim_id to update value of
        interim_id by dropping its instance from the oringinal table and adding its lookup value
        """
        df_old = (
            df_old.drop("interim_id")
            .join(lookup_df.select(F.col("interim_id"), F.col("LSOA11CD")), df_old.lsoa11 == lookup_df.LSOA11CD)
            .drop("lsoa11")
            .withColumnRenamed("LSOA11CD", "lsoa11")
        )
        lookup_df = lookup_df.withColumnRenamed("CIS20CD", "cs20")
        df_new = (
            df_new.drop("interim_id")
            .join(lookup_df.select(F.col("interim_id"), F.col("cs20")).distinct(), lookup_df.cs20 == df_new.CIS20CD)
            .drop("CIS20CD")
            .withColumnRenamed("cs20", "CIS20CD")
        )
        return df_old, df_new

    def combine_with_previous(df_old: DataFrame, df_new: DataFrame):
        """
        combine old and new dataframes by taking matching columns of each dataframe and combining rows
        using unionByName to ensure columns are rearranged
        """
        df_old_missmatches, df_new_missmatches = get_matching_columns(df_old, df_new)
        combined_df = df_old.drop(*df_old_missmatches).unionByName(df_new.drop(*df_new_missmatches))
        return combined_df

    def recode_interim_id(df: DataFrame):
        """
        modify the itnerim id of a dataframe by looking up its uac value in a dictionary
        """
        list = [124761961283, 450215298252, 73579024159]
        df = df.withColumn(
            "interim_id",
            F.when(F.col("uac").isin(list), 118).otherwise(
                F.when(F.col("uac") == 867077909931, 117).otherwise(F.col("interim_id"))
            ),
        )
        return df

    def solve_for_interim_id(df: DataFrame, is_new: bool):
        """
        create new columns within dataframe for useful reference values calulated row-wise with existing column data
        """
        id_window = Window.partitionBy("interim_id")
        df = df.withColumn("sum weights", (F.sum("dweight_hh").over(id_window)))
        df = df.withColumn("CV_dwt", ((F.stddev("dweight_hh").over(id_window) / F.avg("dweight_hh").over(id_window))))
        df = df.withColumn("design_effect", F.lit(1) + F.pow(F.col("CV_dwt"), 2))
        df = df.withColumn("effective_sample_size", F.col("sample_size") / F.col("design_effect"))
        df = df.withColumn("is_new", F.lit(is_new))
        return df

    def calculate_combining_factor(df: DataFrame):
        """
        use a subset of the data partioned by interim id and is_new column to calculate
        the combining facto to be applied to each design weight row-wise
        """
        samples_window = Window.partitionBy("interim_id", "is_new")
        df = df.withColumn("sum_effective_sample_size", (F.sum("effective_sample_size").over(samples_window)))
        df = df.withColumn("combining_factor", (F.col("effective_sample_size") / F.col("sum_effective_sample_size")))
        return df

    def apply_combining_factor(df: DataFrame):
        return df.withColumn("factored_design_weight", F.col("combining_factor") * F.col("dweight_hh"))

    updated_previous_df, updated_new_df = update_interim_id(
        df_old=previous_sample_df, df_new=weighted_new_df, lookup_df=lookup_df
    )
    recoded_previous_df = recode_interim_id(updated_previous_df)
    previous_df_with_sample_size = calculate_sample_size(recoded_previous_df)
    solved_old_df = solve_for_interim_id(previous_df_with_sample_size, False)
    solved_new_df = solve_for_interim_id(updated_new_df, True)
    combined_df = combine_with_previous(df_old=solved_old_df, df_new=solved_new_df)
    summated_effective_sample = calculate_combining_factor(combined_df)
    final = apply_combining_factor(summated_effective_sample)
    final.toPandas().to_csv("final.csv")
    final.show()


combine_design_weights("new_sample_name")


def calibrate_survey_weights():
    pass


def load_weights_to_database():
    pass
