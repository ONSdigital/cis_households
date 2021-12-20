import logging
import sys
from datetime import datetime
from typing import Callable
from typing import List
from typing import Union

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window


def impute_by_distribution(
    df: DataFrame,
    column_name_to_assign: str,
    reference_column: str,
    group_by_columns: List[str],
    first_imputation_value: Union[str, bool, int, float],
    second_imputation_value: Union[str, bool, int, float],
    rng_seed: int = None,
) -> DataFrame:
    """
    Calculate a imputation value from a missing value using a probability
    threshold determined by the proportion of a sub group.

    N.B. Defined only for imputing a binary column.

    Parameters
    ----------
    df
    column_name_to_assign
        The colum that will be created with the impute values
    reference_column
        The column for which imputation values will be calculated
    group_columns
        Grouping columns used to determine the proportion of the reference values
    first_imputation_value
        Imputation value if random number less than proportion
    second_imputation_value
        Imputation value if random number greater than or equal to proportion
    rng_seed
        Random number generator seed for making function deterministic.

    Notes
    -----
    Function provides a column value for each record that needs to be imputed.
    Where the value does not need to be imputed the column value created will be null.
    """
    # .rowsBetween(-sys.maxsize, sys.maxsize) fixes null issues for counting proportions
    window = Window.partitionBy(*group_by_columns).orderBy(reference_column).rowsBetween(-sys.maxsize, sys.maxsize)

    df = df.withColumn(
        "numerator", F.sum(F.when(F.col(reference_column) == first_imputation_value, 1).otherwise(0)).over(window)
    )

    df = df.withColumn("denominator", F.sum(F.when(F.col(reference_column).isNotNull(), 1).otherwise(0)).over(window))

    df = df.withColumn("proportion", F.col("numerator") / F.col("denominator"))

    df = df.withColumn("random", F.rand(rng_seed))

    df = df.withColumn(
        "individual_impute_value",
        F.when(F.col("proportion") > F.col("random"), first_imputation_value)
        .when(F.col("proportion") <= F.col("random"), second_imputation_value)
        .otherwise(None),
    )

    # Make flag easier (non null values will be flagged)
    df = df.withColumn(
        column_name_to_assign,
        F.when(F.col(reference_column).isNull(), F.col("individual_impute_value")).otherwise(None),
    )

    return df.drop("proportion", "random", "denominator", "numerator", "individual_impute_value")


def impute_and_flag(df: DataFrame, imputation_function: Callable, reference_column: str, **kwargs) -> DataFrame:
    """
    Wrapper function for calling imputations, flagging imputed records and recording methods.
    Parameters
    ----------
    df
    imputation_function
        The function that calculates the imputation for a given
        reference column
    reference_column
        The column that will be imputed
    **kwargs
        Key word arguments for imputation_function
    Notes
    -----
    imputation_function is expected to create new column with the values to
    be imputated, and NULL where imputation is not needed.
    """
    df = imputation_function(
        df, column_name_to_assign="temporary_imputation_values", reference_column=reference_column, **kwargs
    )

    status_column = reference_column + "_is_imputed"
    status_other = F.col(status_column) if status_column in df.columns else None

    df = df.withColumn(
        status_column,
        F.when(F.col("temporary_imputation_values").isNotNull(), 1)
        .when(F.col("temporary_imputation_values").isNull(), 0)
        .otherwise(status_other)
        .cast("integer"),
    )

    method_column = reference_column + "_imputation_method"
    method_other = F.col(method_column) if method_column in df.columns else None
    df = df.withColumn(
        reference_column + "_imputation_method",
        F.when(F.col(status_column) == 1, imputation_function.__name__).otherwise(method_other).cast("string"),
    )

    df = df.withColumn(reference_column, F.coalesce(reference_column, "temporary_imputation_values"))

    return df.drop("temporary_imputation_values")


def impute_by_mode(df: DataFrame, column_name_to_assign: str, reference_column: str, group_by_column: str) -> DataFrame:
    """
    Get imputation value from given column by most repeated UNIQUE value
    Parameters
    ----------
    df
    column_name_to_assign
        The column that will be created with the impute values
    reference_column
        The column to be imputed
    group_by_column
        Column name for the grouping
    Notes
    -----
    Function provides a column value for each record that needs to be imputed.
    Where the value does not need to be imputed the column value created will be null.
    """
    value_count_by_group = (
        df.groupBy(group_by_column, reference_column)
        .agg(F.count(reference_column).alias("_value_count"))
        .filter(F.col(reference_column).isNotNull())
    )

    group_window = Window.partitionBy(group_by_column)
    deduplicated_modes = (
        value_count_by_group.withColumn("_is_mode", F.col("_value_count") == F.max("_value_count").over(group_window))
        .filter(F.col("_is_mode"))
        .withColumn("_is_tied_mode", F.count(reference_column).over(group_window) > 1)
        .filter(~F.col("_is_tied_mode"))
        .withColumnRenamed(reference_column, "_imputed_value")
        .drop("_value_count", "_is_mode", "_is_tied_mode")
    )

    imputed_df = (
        df.join(deduplicated_modes, on=group_by_column, how="left")
        .withColumn(column_name_to_assign, F.when(F.col(reference_column).isNull(), F.col("_imputed_value")))
        .drop("_imputed_value")
    )

    return imputed_df


def impute_by_ordered_fill_forward(
    df: DataFrame,
    column_name_to_assign: str,
    column_identity: str,
    reference_column: str,
    order_by_column: str,
    order_type="asc",
) -> DataFrame:
    """
    Impute the last observation of a given field by given identity column.
    Parameters
    ----------
    df
    column_name_to_assign
        The colum that will be created with the impute values
    column_identity
        Identifies any records that the reference_column is missing forward
        This column is normally intended for user_id, participant_id, etc.
    reference_column
        The column for which imputation values will be calculated.
    orderby_column
        the "direction" of the observation will be defined by a ordering column
        within the dataframe. For example: date.
    order_type
        the "direction" of the observation can be ascending by default or
        descending. Chose ONLY 'asc' or 'desc'.
    Notes
    ----
    If the observation carried forward by a specific column like date, and
        the type of order (order_type) is descending, the direction will be
        reversed and the function would do a last observation carried backwards.
    """
    # the id column with a unique monotonically_increasing_id is auxiliary and
    # intends to add an arbitrary number to each row.
    # this will NOT affect the ordering of the rows by orderby_column parameter.
    df = df.withColumn("id", F.monotonically_increasing_id())

    if order_type == "asc":
        ordering_expression = F.col(order_by_column).asc()
    else:
        ordering_expression = F.col(order_by_column).desc()

    window = Window.partitionBy(column_identity).orderBy(ordering_expression)

    return (
        df.withColumn(
            column_name_to_assign,
            F.when(F.col(reference_column).isNull(), F.last(F.col(reference_column), ignorenulls=True).over(window)),
        )
        .orderBy(ordering_expression, "id")
        .drop("id")
    )


def merge_previous_imputed_values(
    df: DataFrame,
    imputed_value_lookup_df: DataFrame,
    id_column_name: str,
) -> DataFrame:
    """
    Retrieve and coalesce imputed values and associated flags from a lookup table.
    Includes the imputed value, imputation status and imputation method.

    Parameters
    ----------
    df
        dataframe to impute values onto
    imputed_value_lookup_df
        previously imputed values to carry forward
    id_column_name
        column that should be used to join previously imputed values on
    """
    imputed_value_lookup_df = imputed_value_lookup_df.toDF(
        *[f"_{column}" if column != id_column_name else column for column in imputed_value_lookup_df.columns]
    )  # _ prevents ambiguity in join, but is sliced out when referencing the original columns

    df = df.join(imputed_value_lookup_df, on=id_column_name, how="left")
    columns_for_editing = [
        (column.replace("_imputation_method", ""), column.replace("_imputation_method", "_is_imputed"), column)
        for column in imputed_value_lookup_df.columns
        if column.endswith("_imputation_method")
    ]

    for value_column, status_column, method_column in columns_for_editing:
        fill_condition = F.col(value_column[1:]).isNull() & F.col(value_column).isNotNull()
        df = df.withColumn(status_column[1:], F.when(fill_condition, F.lit(1)).otherwise(F.lit(0)))
        df = df.withColumn(
            method_column[1:], F.when(fill_condition, F.col(method_column)).otherwise(F.lit(None)).cast("string")
        )
        df = df.withColumn(
            value_column[1:], F.when(fill_condition, F.col(value_column)).otherwise(F.col(value_column[1:]))
        )

    return df.drop(*[name for name in imputed_value_lookup_df.columns if name != id_column_name])


def _create_log(start_time: datetime, log_path: str):
    """Create logger for logging KNN imputation details"""
    log = log_path + "/KNN_imputation_" + start_time.strftime("%d-%m-%Y %H:%M:%S") + ".log"
    logging.basicConfig(
        filename=log, level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%d/%m/%Y %H:%M:%S"
    )
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.info("Started")


def _validate_donor_group_variables(
    df, reference_column, donor_group_columns, donor_group_variable_weights, donor_group_variable_conditions
):
    """
    Validate that donor group column values are within given bounds and data type conditions.
    Also reports summary statistics for group variables.
    """
    # variable to impute not in required impute variables
    if reference_column in donor_group_columns:
        message = "Imputed variable should not be in given impute variables."
        logging.warn(message)
        raise ValueError(message)

    # impute variables and weights are the same length
    if len(donor_group_columns) != len(donor_group_variable_weights):
        message = "Impute weights needs to be the same length as given impute variables."
        logging.warn(message)
        raise ValueError(message)

    df_dtypes = dict(df.dtypes)
    for var in donor_group_variable_conditions.keys():
        if len(donor_group_variable_conditions[var]) != 3:
            message = f"Missing boundary conditions for {var}. Needs to be in format [Min, Max, Dtype]"
            logging.warn(message)
            raise ValueError(message)

        var_min, var_max, var_dtype = donor_group_variable_conditions[var]
        if var_dtype is not None:
            if var_dtype != df_dtypes[var]:
                logging.warn(f"{var} dtype is {df_dtypes[var]} and not the required {var_dtype}")

        if var_min is not None:
            var_min_count = df.filter(F.col(var) > var_min).count()
            if var_min_count > 0:
                logging.warn(f"{var_min_count} rows have {var} below {var_min}" % (var_min_count, var, var_min))

        if var_max is not None:
            var_max_count = df.filter(F.col(var) < var_max).count()
            if var_max_count > 0:
                logging.warn(f"{var_max_count} rows have {var} above {var_max}")

    logging.info("Summary statistics for donor group variables:")
    logging.info(df.select(donor_group_columns).summary().toPandas())


def distance_function(df, impute_vars, impute_weights):
    """Calculate weighted distance between donors and records to be imputed"""
    df = df.withColumn(
        "distance",
        sum(
            [
                (F.col(var) != F.col("don_" + var)).cast(DoubleType()) * impute_weights[i]
                for i, var in enumerate(impute_vars)
            ]
        ),
    )

    min_distances = df.orderBy("distance").groupBy("imp_uniques").agg(F.min("distance").alias("min_distance"))

    df = df.join(min_distances, on="imp_uniques")

    df = df.filter(F.col("distance") <= F.col("min_distance"))
    df.cache().count()
    return df


def impute_by_k_nearest_neighbours(
    df: DataFrame,
    column_name_to_assign: str,
    reference_column: str,
    donor_group_columns: list,
    id_column_name: str,
    log_file_path: str,
    minimum_donors: int = 1,
    donor_group_variable_weights: list = None,
    donor_group_variable_conditions: dict = None,
):
    """
    Minimal PySpark implementation of RBEIS, for K-nearest neighbours imputation.

    Parameters
    ----------
    df
    column_name_to_assign:
        column to store imputed values
    reference_column:
        column that missing values should be imputed for
    donor_group_columns:
        variables used to form unique donor groups to impute from
    donor_group_variable_weights:
        list of weights per ``donor_group_variables``
    donor_group_variable_conditions:
        list of boundary and data type conditions per ``donor_group_variables``
        in the form "variable": [minimum, maximum, "dtype"]
    log_path:
        location the log file is written to
    id_column_name:
        column name of each records unique identifier
    minimum_donors:
        minimum number of donors required in each imputation pool, must be >= 0
    """

    if reference_column not in df.columns:
        message = " Variable to impute ({reference_column}) is not in in columns."
        raise ValueError(message)

    if not all(column in df.columns for column in donor_group_columns):
        message = f"Imputation variables ({donor_group_columns}), should be in dataset columns."
        raise ValueError(message)

    imp_df = df.filter((F.col(reference_column).isNull()) | (F.isnan(reference_column)))
    don_df = df.filter((F.col(reference_column).isNotNull()) & ~(F.isnan(reference_column)))
    df_length = df.count()
    impute_count = imp_df.count()
    donor_count = don_df.count()

    assert impute_count + donor_count == df_length, "Imp df and Don df do not sum to df"

    if impute_count == 0:
        return df

    _create_log(start_time=datetime.now(), log_path=log_file_path)

    logging.info(f"Dataframe length: {df_length}")
    logging.info(f"Impute dataframe length: {impute_count}")
    logging.info(f"Donor dataframe length: {donor_count}")

    if donor_group_variable_weights is None:
        donor_group_variable_weights = [1] * len(donor_group_columns)
        logging.warn(f"No imputation weights specified, using default: {donor_group_variable_weights}")

    if donor_group_variable_conditions is None:
        donor_group_variable_conditions = {var: [None, None, None] for var in donor_group_columns}
        logging.warn(f"No bounds for impute variables specified, using default: {donor_group_variable_conditions}")

    logging.info(f"Function parameters:\n{locals()}")

    _validate_donor_group_variables(
        df, reference_column, donor_group_columns, donor_group_variable_weights, donor_group_variable_conditions
    )

    imp_df = imp_df.withColumn("imp_uniques", F.concat(*donor_group_columns))
    don_df = don_df.withColumn("don_uniques", F.concat(*donor_group_columns))

    # unique groupings of impute vars
    imp_df_unique = imp_df.dropDuplicates(donor_group_columns).select(donor_group_columns + ["imp_uniques"])
    don_df_unique = don_df.dropDuplicates(donor_group_columns).select(donor_group_columns + ["don_uniques"])

    for var in donor_group_columns + [reference_column]:
        don_df_unique = don_df_unique.withColumnRenamed(var, "don_" + var)
        don_df = don_df.withColumnRenamed(var, "don_" + var)

    joined_uniques = imp_df_unique.crossJoin(don_df_unique)

    joined_uniques = joined_uniques.repartition("imp_uniques")

    joined_uniques = distance_function(joined_uniques, donor_group_columns, donor_group_variable_weights)

    candidates = joined_uniques.select("imp_uniques", "don_uniques")

    # only counting one row for matching imp_vars
    freqs = don_df.groupby("don_uniques", "don_" + reference_column).count().withColumnRenamed("count", "frequency")
    freqs = freqs.join(candidates, on="don_uniques")
    freqs = freqs.join(freqs.groupby("imp_uniques").agg(F.sum("frequency").alias("donor_count")), on="imp_uniques")

    # check minimum number of donors
    single_donors = freqs.filter(F.col("donor_count") <= minimum_donors).count()
    if single_donors > 0:
        message = f"{single_donors} donor pools found with less than the required {minimum_donors} minimum donor(s)"
        logging.warn(message)
        logging.warn(freqs.filter(F.col("donor_count") <= minimum_donors).toPandas())
        raise ValueError(message)

    freqs = freqs.withColumn("probs", F.col("frequency") / F.col("donor_count"))

    # best don uniques to get donors from
    full_df = freqs.join(
        imp_df.groupby("imp_uniques").agg(F.count("imp_uniques").alias("imp_group_size")), on="imp_uniques"
    )

    full_df = full_df.withColumn("exp_freq", F.col("probs") * F.col("imp_group_size"))
    full_df = full_df.withColumn("int_part", F.floor(F.col("exp_freq")))
    full_df = full_df.withColumn("dec_part", F.col("exp_freq") - F.col("int_part"))

    # Integer part
    full_df = full_df.withColumn("new_imp_group_size", F.col("imp_group_size") - F.col("int_part"))
    int_dons = full_df.select("int_part", "imp_uniques", "don_" + reference_column).filter(F.col("int_part") >= 1)

    # required decimal donors
    dec_dons = full_df.select("probs", "imp_uniques", "don_" + reference_column, "new_imp_group_size").filter(
        F.col("dec_part") > 0
    )

    # Decimal part
    dec_dons = dec_dons.withColumn("random_number", F.rand() * F.col("probs"))
    tt = Window.partitionBy("imp_uniques").orderBy(F.col("random_number").desc())
    dec_dons = dec_dons.withColumn("row", F.row_number().over(tt)).filter(F.col("row") <= F.col("new_imp_group_size"))

    to_impute = int_dons.select("imp_uniques", "don_" + reference_column).unionByName(
        dec_dons.select("imp_uniques", "don_" + reference_column)
    )
    ww = Window.partitionBy("imp_uniques").orderBy(F.col("rand"))
    to_impute = to_impute.withColumn("rand", F.rand())
    to_impute = to_impute.withColumn("row", F.row_number().over(ww))
    to_impute = to_impute.withColumnRenamed("don_" + reference_column, column_name_to_assign)

    don_df_final = df.filter((F.col(reference_column).isNotNull()) & ~(F.isnan(reference_column)))
    don_df_final.withColumn(column_name_to_assign, F.lit(None).cast(df.schema[reference_column].dataType))
    xx = Window.partitionBy("imp_uniques").orderBy(F.col(id_column_name))
    imp_df = imp_df.withColumn("row", F.row_number().over(xx)).drop(reference_column)

    imp_df_final = imp_df.join(
        to_impute, on=(imp_df.imp_uniques == to_impute.imp_uniques) & (imp_df.row == to_impute.row)
    ).drop("imp_uniques", "row", "rand")

    imputed_count = imp_df_final.count()

    logging.info(f"{imputed_count} records imputed.")
    logging.info(f"Summary statistics for imputed values: {column_name_to_assign}")
    logging.info(imp_df_final.select(column_name_to_assign).summary().toPandas())

    output_df = imp_df_final.unionByName(don_df_final)

    logging.info(f"Summary statistics for donor values: {reference_column}")
    logging.info(don_df_final.select(reference_column).summary().toPandas())

    output_df.cache().count()
    output_df_length = output_df.count()
    if output_df_length != df_length:
        raise ValueError("Records have been lost during imputation")

    missing_count = output_df.filter(
        ((F.col(reference_column).isNull()) | (F.isnan(reference_column)))
        & ((F.col(column_name_to_assign).isNull()) | (F.isnan(column_name_to_assign)))
    ).count()
    if missing_count != 0:
        raise ValueError(f"{missing_count} records still have missing {reference_column} after imputation")

    logging.info("Finished")
    return output_df


def impute_latest_date_flag(df: DataFrame, window_columns: List[str], imputation_flag_columns: str):
    """
    Parameters
    ----------

    """
    window = Window.partitionBy(window_columns).orderBy(F.desc("visit_date"), F.desc("visit_id"))

    df = df.withColumn(
        imputation_flag_columns,
        F.when(
            (
                (F.col("contact_any_covid") == 1)
                & (F.lag("contact_any_covid", 1))
                & (F.col("contact_any_covid_date").isNull())
            )
            | (
                (F.col("contact_any_covid_date") < F.lag("contact_any_covid_date", 1))
                & (F.col("visit_date") >= F.lag("contact_any_covid_date", 1))
            ),
            1,
        ).otherwise(None),
    ).over(window)
    return df
