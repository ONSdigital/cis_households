# from typing import List
# from typing import Union
# from pyspark.sql import DataFrame
# from pyspark.sql import functions as F
# from pyspark.sql import SparkSession
# from pyspark.sql.window import Window
# from cishouseholds.derive import assign_from_lookup
# from cishouseholds.derive import assign_named_buckets
# # 1167
# def chose_scenario_of_dweight_for_antibody_different_household(
#     df: DataFrame,
#     tranche_eligible_indicator: bool,  # TODO: what format is it?
# ) -> Union[str, None]:
#     """
#     Decide what scenario to use for calculation of the design weights
#     for antibodies for different households
#     Parameters
#     ----------
#     df
#     eligibility_pct
#     tranche_eligible_indicator
#     household_samples_dataframe
#     n_eligible_hh_tranche_bystrata_column
#     n_sampled_hh_tranche_bystrata_column
#     """
#     df = df.withColumn(
#         "eligibility_pct",
#         F.when(
#             F.col("number_eligible_households_tranche_bystrata").isNull()
#             & F.col("number_sampled_households_tranche_bystrata").isNull(),
#             0,
#         )
#         .when(
#             F.col("number_eligible_households_tranche_bystrata").isNotNull()
#             & F.col("number_sampled_households_tranche_bystrata").isNotNull()
#             & (F.col("number_sampled_households_tranche_bystrata") > 0),
#             (
#                 100
#                 * (
#                     F.col("number_eligible_households_tranche_bystrata")
#                     - F.col("number_sampled_households_tranche_bystrata")
#                 )
#                 / F.col("number_sampled_households_tranche_bystrata")
#             ),
#         )
#         .otherwise(None),  # TODO: check this
#     )
#     if not tranche_eligible_indicator:  # TODO: not in household_samples_dataframe?
#         return "A"
#     else:
#         if df.select("eligibility_pct").collect()[0][0] == 0.0:
#             return "B"
#         else:
#             return "C"
# # 1168 - test done stefen
# def raw_dweight_for_AB_scenario_for_antibody(
#     df: DataFrame,
#     hh_dweight_antibodies_column,
#     raw_dweight_antibodies_column,
#     sample_new_previous_column,
#     scaled_dweight_swab_nonadjusted_column,
# ) -> DataFrame:
#     """
#     Parameters
#     ----------
#     df
#     hh_dweight_antibodies_column
#     raw_dweight_antibodies_column
#     sample_new_previous_column
#     scaled_dweight_swab_nonadjusted_column
#     """
#     df = df.withColumn(
#         raw_dweight_antibodies_column,
#         F.when(F.col(hh_dweight_antibodies_column).isNotNull(), F.col(hh_dweight_antibodies_column)),
#     )
#     df = df.withColumn(
#         raw_dweight_antibodies_column + "_b",  # TODO: should this be AB
#         F.when(
#             (F.col(sample_new_previous_column) == "previous") & (F.col(hh_dweight_antibodies_column).isNotNull()),
#             F.col(hh_dweight_antibodies_column),
#         ).when(
#             (F.col(sample_new_previous_column) == "new") & (F.col(hh_dweight_antibodies_column).isNull()),
#             F.col(scaled_dweight_swab_nonadjusted_column),
#         ),
#     )
#     df = df.withColumn(
#         raw_dweight_antibodies_column,
#         F.when(hh_dweight_antibodies_column).isNull(),
#         F.col(scaled_dweight_swab_nonadjusted_column),
#     )
#     return df
# # 1169 - test done stefen
# def function_name_1169(
#     df: DataFrame,
#     sample_new_previous: str,
#     tranche_eligible_hh: str,
#     tranche_n_indicator: str,
#     raw_dweight_antibodies_c: str,
#     scaled_dweight_swab_nonadjusted: str,
#     tranche_factor: str,
#     hh_dweight_antibodies_c: str,
#     dweights_swab: str,
# ) -> DataFrame:
#     """
#     1 step: for cases with sample_new_previous = "previous" AND tranche_eligible_households=yes(1)
#     AND  tranche_number_indicator = max value, calculate raw design weight antibodies by using
#     tranche_factor
#     2 step: for cases with sample_new_previous = "previous";  tranche_number_indicator != max value;
#     tranche_ eligible_households != yes(1), calculate the  raw design weight antibodies by using
#     previous dweight antibodies
#     3 step: for cases with sample_new_previous = new, calculate  raw design weights antibodies
#     by using  design weights for swab
#     Parameters
#     ----------
#     df
#     sample_new_previous
#     tranche_eligible_hh
#     tranche_n_indicator
#     raw_dweight_antibodies_c
#     scaled_dweight_swab_nonadjusted
#     tranche_factor
#     hh_dweight_antibodies_c
#     dweights_swab
#     """
#     max_value = df.agg({"tranche": "max"}).first()[0]
#     df = df.withColumn(
#         "raw_design_weight_antibodies",
#         F.when(
#             (F.col(sample_new_previous) == "previous")
#             & (F.col(tranche_eligible_hh) == "Yes")
#             & (F.col(tranche_n_indicator) == max_value),
#             F.col(scaled_dweight_swab_nonadjusted) * F.col(tranche_factor),
#         )
#         .when(
#             (F.col(sample_new_previous) == "previous")
#             & (F.col(tranche_n_indicator) != max_value)
#             & (F.col(tranche_eligible_hh) != "Yes"),
#             F.col(hh_dweight_antibodies_c),
#         )
#         .when(
#             (F.col(sample_new_previous) == "new")
#             & (F.col(raw_dweight_antibodies_c) == F.col(scaled_dweight_swab_nonadjusted)),
#             F.col(dweights_swab),
#         ),
#     )
#     return df
# # 1170 test done stefen
# def function_name_1170(df: DataFrame, dweights_column: str, carryforward_dweights_antibodies_column: str) -> DataFrame: # noqa: E501
#     """
#     Bring the design weights calculated up until this point  into same variable: carryforward_dweights_antibodies.
#     Scaled up to population level  carryforward design weight antibodies.
#     Parameters
#     ----------
#     df
#     dweights_column
#     carryforward_dweights_antibodies_column
#     """
#     # part 1: bring the design weights calculated up until this point  into same variable:
#     #  carryforward_dweights_antibodies
#     df = df.withColumn(carryforward_dweights_antibodies_column, F.col(dweights_column))  # TODO
#     # part 2: scaled up to population level  carryforward design weight antibodies
#     return df
# # 1171 stefen has done test
# def function_name_1171(
#     df: DataFrame,
#     dweights_antibodies_column: str,
#     total_population_column: str,
#     cis_area_code_20_column: str,
# ) -> DataFrame:
#     """
#     check the dweights_antibodies_column are adding up to total_population_column.
#     check the design weights antibodies are all are positive.
#     check there are no missing design weights antibodies.
#     check if they are the same across cis_area_code_20 by sample groups (by sample_source).
#     Parameters
#     ----------
#     df
#     """
#     # part 1: check the dweights_antibodies_column are adding up to total_population_column
#     assert sum(list(df.select(dweights_antibodies_column).toPandas()[dweights_antibodies_column])) == sum(
#         list(df.select(total_population_column).toPandas()[total_population_column])
#     )
#     df = df.withColumn(
#         "check_2-dweights_positive_notnull",
#         F.when(F.col(total_population_column) > 1, 1).when(F.col(total_population_column).isNotNull(), 1),
#     )
#     # TODO part 4: check if they are the same across cis_area_code_20 by sample groups (by sample_source)
#     return df
