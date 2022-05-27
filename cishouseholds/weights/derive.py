import re
from typing import Any
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from cishouseholds.derive import assign_distinct_count_in_group


def assign_ethnicity_white(
    df: DataFrame, column_name_to_assign: str, country_column: str, ethnicity_column_ni: str, ethnicity_column: str
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
                | ((F.col(country_column) != "northern ireland"))
                & (
                    (F.col(ethnicity_column) == "white british")
                    | (F.col(ethnicity_column) == "white irish")
                    | (F.col(ethnicity_column) == "other white")
                )
            ),
            "Yes",
        ).otherwise("No"),
    )
    return df


def assign_white_proportion(
    df: DataFrame,
    column_name_to_assign: str,
    white_bool_column: str,
    country_column: str,
    weight_column: str,
    age_column: str,
):
    """
    Assign a column with calculated proportion of white adult population by country
    """
    window = Window.partitionBy(country_column)
    df = df.withColumn(
        column_name_to_assign,
        F.sum(
            F.when(((F.col(white_bool_column) == "yes") & (F.col(age_column) > 16)), F.col(weight_column)).otherwise(0)
        ).over(window)
        / (F.sum(weight_column).over(window)),
    )
    return df


def assign_population_projections(
    current_projection_df: DataFrame, previous_projection_df: DataFrame, month: int, m_f_columns: List[str]
):
    """
    Use a given input month to create a scalar between previous and current values within m and f
    subscript columns and update values accordingly
    """
    for col in m_f_columns:
        current_projection_df = current_projection_df.withColumnRenamed(col, f"{col}_new")
    current_projection_df = current_projection_df.join(
        previous_projection_df.select("id", *m_f_columns), on="id", how="left"
    )
    if month < 6:
        a = 6 - month
        b = 6 + month
    else:
        a = 18 - month
        b = month - 6

    for col in m_f_columns:
        current_projection_df = current_projection_df.withColumn(
            col, F.lit(1 / 12) * ((a * F.col(col)) + (b * F.col(f"{col}_new")))
        )
        current_projection_df = current_projection_df.drop(f"{col}_new")
    return current_projection_df


def derive_m_f_column_list(df: DataFrame):
    r = re.compile(r"\w{1}\d{1,}")
    return [item for item in list(filter(r.match, df.columns)) if item in df.columns]


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


def assign_sample_new_previous(df: DataFrame, column_name_to_assign: str, date_column: str, batch_number_column: str):
    """
    Assign as new sample where batch number is the highest in the dataset, otherwise assign as previous.
    """
    df = df.withColumn(
        column_name_to_assign,
        F.when(
            ((F.col(date_column) == F.max(date_column)) & (F.col(batch_number_column) == F.max(batch_number_column))),
            "new",
        ).otherwise("previous"),
    )
    return df


def count_distinct_in_filtered_df(
    df: DataFrame,
    column_name_to_assign: str,
    column_to_count: str,
    filter_positive: Any,
    group_by_columns: List[str],
):
    """
    Count distinct rows that meet a given condition over a predefined window (window)
    """
    eligible_df = df.filter(filter_positive)
    eligible_df = assign_distinct_count_in_group(
        eligible_df, column_name_to_assign, [column_to_count], group_by_columns
    )
    ineligible_df = df.filter(~filter_positive).withColumn(column_name_to_assign, F.lit(0))
    df = eligible_df.unionByName(ineligible_df)
    return df


def assign_tranche_factor(
    df: DataFrame,
    column_name_to_assign: str,
    barcode_column: str,
    barcode_ref_column: str,
    tranche_column: str,
    group_by_columns: List[str],
):
    """
    Assign a variable tranche factor as the ratio between 2 derived columns
    (number_eligible_households_tranche_by_strata_enrolment),
    (number_sampled_households_tranche_by_strata_enrolment) when the household is eligible to be sampled
    as the barcode column is not null and the tranche
    value is maximum within the predefined window (window)
    """
    df = df.withColumn("tranche_eligible_households", F.when(F.col(barcode_ref_column).isNull(), "No").otherwise("Yes"))
    df = count_distinct_in_filtered_df(
        df=df,
        column_name_to_assign="number_eligible_households_tranche_by_strata_enrolment",
        column_to_count=barcode_column,
        filter_positive=F.col("tranche_eligible_households") == "Yes",
        group_by_columns=group_by_columns,
    )

    filter_max_condition = (F.col("tranche_eligible_households") == "Yes") & (
        F.col(tranche_column) == df.agg({tranche_column: "max"}).first()[0]
    )
    df = count_distinct_in_filtered_df(
        df=df,
        column_name_to_assign="number_sampled_households_tranche_by_strata_enrolment",
        column_to_count=barcode_column,
        filter_positive=filter_max_condition,
        group_by_columns=group_by_columns,
    )
    df = df.withColumn(
        column_name_to_assign,
        F.when(
            filter_max_condition,
            F.col("number_eligible_households_tranche_by_strata_enrolment")
            / F.col("number_sampled_households_tranche_by_strata_enrolment"),
        ).otherwise("missing"),
    )
    return df.drop(barcode_ref_column)
