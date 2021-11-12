import sys
from typing import Callable
from typing import List
from typing import Union

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
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


##############################
# Pyspark RBEIS Method
##############################

"""
# Author: Alex Lewis
# Email: Alex.d.lewis@ons.gov.uk
"""
import pyspark
import pyspark.sql.functions as f
from pyspark.sql.functions import col, isnan, concat, countDistinct, floor,row_number
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType, StringType
import pandas as pd
import numpy as np
import datetime
import logging

#################
# Begin QA Funcs
#################

###########
# Reporting
###########


def create_log(start_time, log_path=log_path):
  log = log_path+start_time+".log"
  logging.basicConfig(filename=log,level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s', datefmt='%d/%m/%Y %H:%M:%S')
  logging.getLogger("requests").setLevel(logging.WARNING)
  logging.info("Started")
  pass


def init_log_vars(imp_var, impute_vars, impute_weights):
  logging.info("Variable to impute: %s" % imp_var)
  logging.info("Using imputation variables: %s" % str(impute_vars))
  logging.info("With respective weightings: %s" % str(impute_weights))
  pass

def report_to_log(message, print_=False, warn=False):
  if print_:
    print(message)
  if not warn: 
    logging.info(message)
  else:
    logging.warning(message)
  pass

############################
# begin sparksession helper
############################

def SPARK():
  ss = SparkSession.builder.appName("CIS_RBEIS")\
  .config('spark.ui.showConsoleProgress', 'false')\
  .getOrCreate()
  
  ss.sparkContext.setLogLevel('WARN')
  
  return ss



################
# Q A Functions
################


def test_program(df, imp_var, impute_vars, kwargs):
   # test df, imp_var, impute_vars
  test_df(df, imp_var, impute_vars)
  
  if "impute_weights" not in kwargs:
      report_to_log("No imputation weights specified, using default: %s" % str([1]*len(impute_vars)), warn=True)
  impute_weights = kwargs.pop("impute_weights", [1]*len(impute_vars))

  init_log_vars(imp_var, impute_vars, impute_weights)
  df, kwargs = test_imp_vars(df, imp_var, impute_vars, impute_weights, kwargs)
  
  id_column, min_donors = test_kwargs(df, kwargs)
  
  return df, imp_var, impute_vars, impute_weights, id_column, min_donors



def test_df(df, imp_var, impute_vars):
  # check df
  if not type(df) == pyspark.sql.dataframe.DataFrame:
    report_to_log("Dataframe given is not required pyspark.sql.dataframe.DataFrame but %s" % type(df), warn=True)
    raise ValueError("Dataframe given is not required pyspark.sql.dataframe.DataFrame but %s" % type(df))
  
  if not imp_var in df.columns:
    report_to_log("Variable to impute should be in dataset columns.", warn=True)
    raise ValueError("Variable to impute, \"%s\", should be in dataset columns." % imp_var)

  if not all(impute_var in df.columns for impute_var in impute_vars):
    report_to_log("Imputation variables, \"%s\", should be in dataset columns." % (impute_vars),warn=True)
    raise ValueError("Imputation variables, \"%s\", should be in dataset columns." % (impute_vars))

  pass


def test_imp_vars(df, imp_var, impute_vars, impute_weights, kwargs):
   # variable to impute not in required impute variables
  if imp_var in impute_vars: 
    report_to_log("Imputed variable should not be in given impute variables.",warn=True)
    raise ValueError("Imputed variable should not be in given impute variables.")
      
    # impute variables and weights are the same length
  if len(impute_vars)!=len(impute_weights):
    report_to_log("Impute weights needs to be the same length as given impute variables.",warn=True)
    raise ValueError("Impute weights needs to be the same length as given impute variables.")
  
  if "impute_vars_conditions" not in kwargs:
    report_to_log("No bounds for impute variables specified, using default: [None,None,None]", warn=True)
  impute_vars_conditions = kwargs.pop("impute_vars_conditions", {var: [None,None,None] for var in impute_vars})

  df_dtypes = dict(df.dtypes)
  for var in impute_vars_conditions.keys():

    if len(impute_vars_conditions[var])!=3:
      report_to_log("Missing boundary conditions for %s. Needs to be in format [Min, Max, Dtype]" % var, warn=True)
      raise ValueError("Missing boundary conditions for %s. Needs to be in format [Min, Max, Dtype]" % var)

    var_min, var_max, var_dtype = impute_vars_conditions[var]
    if var_dtype is not None:
      if var_dtype != df_dtypes[var]:
        report_to_log("%s dtype is %s and not the required %s so trying to change dtype" % (var, df_dtypes[var], var_dtype) ,warn=True)
        try:
          df = df.withColumn(var,col(var).cast(var_dtype))
        except:
          report_to_log("Failed to convert %s dtype %s to %s" % (var, df_dtypes[var], var_dtype), warn=True)
    else:
      report_to_log("%s dtype is None so no dtype is enforced" % var, warn=True)


    if var_min is not None:
      var_min_count = df.filter(col(var) > var_min).count()
      if var_min_count > 0:
        report_to_log("%d rows have %s below %d" % (var_min_count, var, var_min), warn=True)
    else:
      report_to_log("%s minimum is None so no lower boundary is enforced" % var, warn=True)

    if var_max is not None:
      var_max_count = df.filter(col(var) < var_max).count()
      if var_max_count > 0:
        report_to_log("%d rows have %s above %d" % (var_max_count, var, var_max), warn=True)
    else:
      report_to_log("%s maximum is None so no upper boundary is enforced" % var, warn=True)

  report_to_log("Summary statistics for imputation variables "+str(impute_vars))
  report_to_log(df.select(impute_vars).summary().toPandas())

  return df, kwargs


def test_kwargs(df, kwargs):
  if "id_column" not in kwargs:
    report_to_log("No id column specified, using default: \"participant_id\"", warn=True)
  id_column = kwargs.pop("id_column", "participant_id")  

  if id_column not in df.columns:
    report_to_log("id_column, \"%s\", not in df columns" % id_column, warn=True)
    raise ValueError("id_column, \"%s\", not in df columns" % id_column)
  
  if "min_donors" not in kwargs:
    report_to_log("No min donors specified, using default: 1", warn=True)
  min_donors = kwargs.pop("min_donors", 1)
  
  if min_donors < 0:
    report_to_log("Min donors can't be negative!", warn=True)
    raise ValueError("Min donors can't be negative!")
    
  if kwargs:
    raise TypeError('Unexpected **kwargs: %r' % kwargs)
    
  return id_column, min_donors

#############
# begin main
#############


def distance_function(df, impute_vars, impute_weights):
  df = df.withColumn("distance", sum([(col(var)!=col("don_"+var)).cast(DoubleType())*impute_weights[i] for i,var in enumerate(impute_vars)]))

  min_distances = df.orderBy("distance").groupBy("imp_uniques")\
  .agg(f.min("distance").alias("min_distance"))

  df = df.join(min_distances, on='imp_uniques')
  
  # filter min distances
  df = df.filter(col("distance")<=col("min_distance"))
  df.cache().count()
  return df

def impute_method(df, imp_var, impute_vars, **kwargs):
  """
  df: Pyspark dataframe
  imp_var: imputation variable
  impute_vars: variables used to form unique donor groups to impute from
  **kwargs: dictionary of key word arguments in the form {"variable":value}
  
  List of possible kwargs:
  "impute_weights" : list of weights of each impute_vars
  "impute_vars_conditions": list of boundary and data type conditions - in the form [minimum, maximum, "dtype"]

  "log_path" : location the logging file is outputted to
  "id_column": column name of each records unique identifier
  "min_donors": minimum donors required in each imputation pool
  """
  
  # start time
  start_time = datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S')
  
  # check sparksession is running
  sparkSession = SPARK()
    
  log_path = kwargs.pop("log_path", "")
  create_log(start_time,log_path=log_path)
  
  df, imp_var, impute_vars, impute_weights,\
  id_column, min_donors = test_program(df, imp_var, impute_vars, kwargs)
 
  
  # create imputation and donor sets
  # where imputation are those missing imp_var
  # and donor are those with valid imp_var

  imp_df = df.filter((col(imp_var).isNull())|(isnan(imp_var)))
  don_df = df.filter((col(imp_var).isNotNull())&~(isnan(imp_var)))
  import_count = df.count()
  impute_count = imp_df.count()
  donor_count = don_df.count()
  
  report_to_log("Dataframe length: %s" % import_count)
  report_to_log("Impute dataframe length: %s" % impute_count)
  report_to_log("Donor dataframe length: %s" % donor_count)
  
  if impute_count == 0:
    # send nothing to impute to log
    report_to_log("No Null values found in %s." % imp_var, warn=True)
    return df
    
  # CHECK: imp_df + don_df should sum to df
  assert impute_count+donor_count==import_count, "Imp df and Don df do not sum to df"

  imp_df = imp_df.withColumn("imp_uniques", concat(*impute_vars))
  don_df = don_df.withColumn("don_uniques", concat(*impute_vars))

  # unique groupings of impute vars
  imp_df_unique = imp_df.dropDuplicates(impute_vars).select(impute_vars+["imp_uniques"])
  don_df_unique = don_df.dropDuplicates(impute_vars).select(impute_vars+["don_uniques"])

  # and for each unique combination
  # create df of candidates

  for var in impute_vars+[imp_var]:
    don_df_unique = don_df_unique.withColumnRenamed(var, "don_"+var)
    don_df = don_df.withColumnRenamed(var, "don_"+var)

  joined_uniques = imp_df_unique.crossJoin(don_df_unique)
  
  joined_uniques = joined_uniques.repartition("imp_uniques")
  
  # distance function
  joined_uniques = distance_function(joined_uniques, impute_vars, impute_weights)
  
  candidates = joined_uniques.select("imp_uniques","don_uniques")

  # only counting one row for matching imp_vars
  freqs = don_df.groupby("don_uniques","don_"+imp_var)\
  .count().withColumnRenamed("count","frequency")

  freqs = freqs.join(candidates, on="don_uniques")

  freqs = freqs.join(freqs.groupby("imp_uniques").agg(f.sum("frequency").alias("donor_count")),on="imp_uniques")

  # check minimum number of donors
  single_donors = freqs.filter(col("donor_count")<=min_donors).count()
  if single_donors>0:
    report_to_log("%d donor pools found with less than the required %s minimum donor(s)" % (single_donors, min_donors), warn=True)
    
    report_to_log(freqs.filter(col("donor_count")<=min_donors).toPandas())    
    raise ValueError("%d donor pools found with less than the required %s minimum donor(s)" % (single_donors, min_donors))

  
  freqs = freqs.withColumn("probs", col("frequency")/col("donor_count"))


  # best don uniques to get donors from
  full_df = freqs.join(imp_df.groupby("imp_uniques").agg(f.count("imp_uniques").alias("imp_group_size")), on="imp_uniques")

  full_df = full_df.withColumn("exp_freq", col("probs")*col("imp_group_size"))

  full_df = full_df.withColumn("int_part", floor(col("exp_freq")))

  full_df = full_df.withColumn("dec_part", col("exp_freq")-col("int_part"))


  ###########  
  # int part
  ###########

  # take don imp_var where int part !=0
  full_df = full_df.withColumn("new_imp_group_size", col("imp_group_size")-col("int_part"))
  int_dons = full_df.select("int_part","imp_uniques","don_"+imp_var).filter(col("int_part")>=1)

  # required decimal donors 
  dec_dons = full_df.select("probs","imp_uniques","don_"+imp_var,"new_imp_group_size").filter(col("dec_part")>0)


  ###########
  # dec part
  ###########

  # generate random number 
  # times by prob
  # sort by prob
  # select number required
  dec_dons = dec_dons.withColumn("random_number", f.rand()*col("probs") )  
  tt = Window.partitionBy("imp_uniques").orderBy(col("random_number").desc())
  dec_dons = dec_dons.withColumn("row",row_number().over(tt)) \
    .filter(col("row") <=col("new_imp_group_size")) 


  # join int_dons and dec_dons
  to_impute = int_dons.select("imp_uniques","don_"+imp_var).unionByName(dec_dons.select("imp_uniques","don_"+imp_var))
  ww = Window.partitionBy("imp_uniques").orderBy(col("rand"))
  to_impute = to_impute.withColumn("rand", f.rand())
  to_impute = to_impute.withColumn("row", row_number().over(ww))
  to_impute = to_impute.withColumnRenamed("don_"+imp_var,imp_var)


  don_df_final = df.filter((col(imp_var).isNotNull())&~(isnan(imp_var)))
  xx = Window.partitionBy("imp_uniques").orderBy(col(id_column))
  imp_df = imp_df.withColumn("row", row_number().over(xx)).drop(imp_var)

  imp_df_final = imp_df.join(to_impute, on=(imp_df.imp_uniques==to_impute.imp_uniques)\
                             &(imp_df.row==to_impute.row)).drop("imp_uniques","row","rand")

  # prepare export df
  export_df = sparkSession.createDataFrame([],df.schema)
  export_df = export_df.unionByName(imp_df_final)
  imputed_count = export_df.count()
  
  report_to_log("%s records imputed." % imputed_count)
  report_to_log("Summary statistics for imputed values: %s" % imp_var)
  report_to_log(imp_df_final.select(imp_var).summary().toPandas())
  
  export_df = export_df.unionByName(don_df_final)
  
  report_to_log("Summary statistics for donor values: %s" % imp_var)
  report_to_log(don_df_final.select(imp_var).summary().toPandas())

  # output
  # check output size
  export_df.cache().count()
  export_count=export_df.count()
  assert(export_count==import_count, "Records have been lost!")
  report_to_log("%s records exported." % export_count)
  
  # check nans are removed
  missing_count = export_df.filter((col(imp_var).isNull())|(isnan(imp_var))).count()
  assert(missing_count==0, "%s records still have missing %s" % (missing_count, imp_var))
  
  
  #export 
  report_to_log("Finished")
  return export_df








