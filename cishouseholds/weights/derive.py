from typing import Any
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def assign_ethnicity_white(
    df: DataFrame, column_name_to_assign: str, country_column: str, ethnicity_column_ni: str, ethnicity_column
):
    """
    Assign boolean (yes / no) column to show if participant ethnicity is white
    """
    df = df.withColumn(
        column_name_to_assign,
        F.when(
            (
                (F.col(country_column) == "northern ireland")
                & ((F.col(ethnicity_column_ni) == "white") | (F.col(ethnicity_column_ni) == "irish traveller"))
                | ((F.col(country_column) != "northern ireland")) & ((F.col(ethnicity_column) == "white british")|(F.col(ethnicity_column)=="white irish")|(F.col(ethnicity_column)=="other white"))
            ),
            "yes",
        ).otherwise("no"),
    )
    return df

def assign_white_proportion(df:DataFrame, column_name_to_assign:str, white_bool_column:str, country_column:str, weight_column:str, age_column:str):
    """
    Assign a column with calculated proportion of white adult population by country
    """
    window = Window.partitionBy(country_column)
    df = df.withColumn(column_name_to_assign, F.sum(F.when(((F.col(white_bool_column)=="yes")&(F.col(age_column)>16)),F.col(weight_column)).otherwise(0)).over(window)/(F.sum(weight_column).over(window)))
    return df


def get_matches(old_sample_df: DataFrame, new_sample_df: DataFrame, selection_columns: List[str], barcode_column: str):
    """
    assign column to denote whether the data of a given set of columns (selection_columns) matches
    between old and new sample df's (old_sample_df) (new_sample_df)
    """
    select_df = old_sample_df.select(barcode_column, *selection_columns)
    for col in select_df.columns:
        if col != barcode_column:
            select_df = select_df.withColumnRenamed(col, col + "_OLD")
    joined_df = new_sample_df.join(select_df, on=barcode_column, how="left")
    for col in selection_columns:
        joined_df = joined_df.withColumn(f"MATCHED_{col}", F.when(F.col(col) == F.col(f"{col}_OLD"), 1).otherwise(None))
    return joined_df


def assign_sample_new_previous(df: DataFrame, colum_name_to_assign: str, date_column: str, batch_colum: str):
    """
    Assign column by checking for highest batch number in most recent date where new is value if true
    and previous otherwise
    """
    window = Window.partitionBy(date_column).orderBy(date_column, F.desc(batch_colum))
    df = df.withColumn("DATE_REFERENCE", F.first(date_column).over(window))
    df = df.withColumn("BATCH_REFERENCE", F.first(batch_colum).over(window))
    df = df.withColumn(
        colum_name_to_assign,
        F.when(
            ((F.col(date_column) == F.col("DATE_REFERENCE")) & (F.col(batch_colum) == F.col("BATCH_REFERENCE"))), "new"
        ).otherwise("previous"),
    )
    return df.drop("DATE_REFERENCE", "BATCH_REFERENCE")


def count_distinct_in_filtered_df(
    df: DataFrame,
    column_name_to_assign: str,
    column_to_count: str,
    filter_positive: Any,
    window: Window,
):
    """
    Count distinct rows that meet a given condition over a predefined window (window)
    """
    eligible_df = df.filter(filter_positive)
    eligible_df = eligible_df.withColumn(column_name_to_assign, F.approx_count_distinct(column_to_count).over(window))
    ineligible_df = df.filter(~filter_positive).withColumn(column_name_to_assign, F.lit(0))
    df = eligible_df.unionByName(ineligible_df)
    return df


def assign_tranche_factor(df: DataFrame, column_name_to_assign: str, barcode_column: str, group_by_columns: List[str]):
    """
    Assign a variable tranche factor as the ratio between 2 derived columns
    (number_eligible_households_tranche_bystrata_enrolment),
    (number_sampled_households_tranche_bystrata_enrolment) when the household is eligible to be sampled and the tranche
    value is maximum within the predefined window (window)
    """
    df = df.withColumn("tranche_eligible_households", F.when(F.col(barcode_column).isNull(), "No").otherwise("Yes"))
    window = Window.partitionBy(*group_by_columns)
    df = count_distinct_in_filtered_df(
        df,
        "number_eligible_households_tranche_bystrata_enrolment",
        barcode_column,
        F.col("tranche_eligible_households") == "Yes",
        window,
    )
    filter_max_condition = (F.col("tranche_eligible_households") == "Yes") & (
        F.col("tranche") == df.agg({"tranche": "max"}).first()[0]
    )
    df = count_distinct_in_filtered_df(
        df,
        "number_sampled_households_tranche_bystrata_enrolment",
        barcode_column,
        filter_max_condition,
        window,
    )
    df = df.withColumn(
        column_name_to_assign,
        F.when(
            filter_max_condition,
            F.col("number_eligible_households_tranche_bystrata_enrolment")
            / F.col("number_sampled_households_tranche_bystrata_enrolment"),
        ).otherwise("missing"),
    )
    return df.drop(
        "number_eligible_households_tranche_bystrata_enrolment", "number_sampled_households_tranche_bystrata_enrolment"
    )
