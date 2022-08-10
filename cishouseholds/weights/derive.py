import re
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


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
    Assign as new sample where date and batch number are highest in the dataset, otherwise assign as previous.
    """
    false_window = Window.partitionBy(F.lit(0))
    df = df.withColumn("MAX_DATE", F.max(date_column).over(false_window))
    df = df.withColumn("MAX_BATCH_NUMBER", F.max(batch_number_column).over(false_window))
    df = df.withColumn(
        column_name_to_assign,
        F.when(
            ((F.col(date_column) == F.col("MAX_DATE")) & (F.col(batch_number_column) == F.col("MAX_BATCH_NUMBER"))),
            "new",
        ).otherwise("previous"),
    )
    return df.drop("MAX_DATE", "MAX_BATCH_NUMBER")


def assign_tranche_factor(
    df: DataFrame,
    tranche_factor_column_name_to_assign: str,
    eligibility_percentage_column_name_to_assign: str,
    household_id_column: str,
    tranche_column: str,
    elibility_column: str,
    strata_columns: List[str],
):
    """
    Assign tranche factor as the ratio between the number of eligible households in the strata
    by the number of eligible households with the maximum tranche value in the strata.

    Outcome is Null for uneligible households.
    """
    eligible_window = Window.partitionBy(elibility_column, *strata_columns)
    eligible_households_by_strata = F.count(F.col(household_id_column)).over(eligible_window)

    tranche_window = Window.partitionBy(elibility_column, tranche_column, *strata_columns)
    latest_tranche_households_by_strata = F.count(F.col(household_id_column)).over(tranche_window)
    df = df.withColumn("latest_tranche_households_by_strata", latest_tranche_households_by_strata)
    df = df.withColumn("eligible_households_by_strata", eligible_households_by_strata)

    df = df.withColumn("MAX_TRANCHE_NUMBER", F.max(tranche_column).over(Window.partitionBy(F.lit(0))))
    latest_tranche = (F.col(elibility_column) == "Yes") & (F.col(tranche_column) == F.col("MAX_TRANCHE_NUMBER"))
    df = df.withColumn(
        tranche_factor_column_name_to_assign,
        F.when(latest_tranche, eligible_households_by_strata / latest_tranche_households_by_strata),
    )
    df = df.withColumn(
        eligibility_percentage_column_name_to_assign,
        F.when(
            latest_tranche,
            (
                (eligible_households_by_strata - latest_tranche_households_by_strata)
                / latest_tranche_households_by_strata
            )
            * 100,
        ),
    )
    return df.drop("MAX_TRANCHE_NUMBER")
