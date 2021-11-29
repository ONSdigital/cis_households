from typing import Union

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# 1167
def chose_scenario_of_dweight_for_antibody_different_household(
    df: DataFrame,
    tranche_eligible_indicator: str,
    # household_samples_dataframe: List[str],
    n_eligible_hh_tranche_bystrata_column,
    n_sampled_hh_tranche_bystrata_column,
) -> Union[str, None]:
    """
    Decide what scenario to use for calculation of the design weights
    for antibodies for different households
    Parameters
    ----------
    df
    tranche_eligible_indicator
    household_samples_dataframe
    n_eligible_hh_tranche_bystrata_column
    n_sampled_hh_tranche_bystrata_column
    """
    if F.col(n_eligible_hh_tranche_bystrata_column).isNull() & F.col(n_sampled_hh_tranche_bystrata_column).isNull():
        eligibility_pct = 0

    elif (
        F.col(n_eligible_hh_tranche_bystrata_column).isNotNull()
        & F.col(n_sampled_hh_tranche_bystrata_column).isNotNull()
        & (F.col(n_sampled_hh_tranche_bystrata_column) > 0)
    ):
        eligibility_pct = (
            100
            * (n_eligible_hh_tranche_bystrata_column - n_sampled_hh_tranche_bystrata_column)
            / n_sampled_hh_tranche_bystrata_column
        )
    if not tranche_eligible_indicator:  # TODO: not in household_samples_dataframe?
        return "A"
    else:
        if eligibility_pct == 0:
            return "B"
        else:
            return "C"


# 1168
def raw_dweight_for_AB_scenario_for_antibody(
    df: DataFrame,
    hh_dweight_antibodies_column,
    raw_dweight_antibodies_column,
    sample_new_previous_column,
    scaled_dweight_swab_nonadjusted_column,
) -> DataFrame:
    """
    Parameters
    ----------
    df
    hh_dweight_antibodies_column
    raw_dweight_antibodies_column
    sample_new_previous_column
    scaled_dweight_swab_nonadjusted_column
    """
    df = df.withColumn(
        raw_dweight_antibodies_column,
        F.when(F.col(hh_dweight_antibodies_column).isNotNull(), F.col(hh_dweight_antibodies_column)),
    )

    df = df.withColumn(
        raw_dweight_antibodies_column + "_b",
        F.when(
            (F.col(sample_new_previous_column) == "previous") & (F.col(hh_dweight_antibodies_column).isNotNull()),
            F.col(hh_dweight_antibodies_column),
        ).when(
            (F.col(sample_new_previous_column) == "new") & (F.col(hh_dweight_antibodies_column).isNull()),
            F.col(scaled_dweight_swab_nonadjusted_column),
        ),
    )
    df = df.withColumn(
        raw_dweight_antibodies_column,
        F.when(hh_dweight_antibodies_column).isNull(),
        F.col(scaled_dweight_swab_nonadjusted_column),
    )
    return df


# 1170
def function_name_1170(df: DataFrame, dweights_column: str, carryforward_dweights_antibodies_column: str) -> DataFrame:
    """
    Bring the design weights calculated up until this point  into same variable: carryforward_dweights_antibodies.
    Scaled up to population level  carryforward design weight antibodies.
    Parameters
    ----------
    df
    dweights_column
    carryforward_dweights_antibodies_column
    """
    # part 1: bring the design weights calculated up until this point  into same variable:
    #  carryforward_dweights_antibodies
    df = df.withColumn(carryforward_dweights_antibodies_column, F.col(dweights_column))  # TODO

    # part 2: scaled up to population level  carryforward design weight antibodies

    return df


# 1171
def function_name_1171(
    df: DataFrame,
    dweights_antibodies_column: str,
    total_population_column: str,
    cis_area_code_20_column: str,
) -> DataFrame:
    """
    check the dweights_antibodies_column are adding up to total_population_column.
    check the design weights antibodies are all are positive.
    check there are no missing design weights antibodies.
    check if they are the same across cis_area_code_20 by sample groups (by sample_source).

    Parameters
    ----------
    df
    """
    # part 1: check the dweights_antibodies_column are adding up to total_population_column
    assert sum(list(df.select(dweights_antibodies_column).toPandas()[dweights_antibodies_column])) == sum(
        list(df.select(total_population_column).toPandas()[total_population_column])
    )

    df = df.withColumn(
        "check_2-dweights_positive_notnull",
        F.when(F.col(total_population_column) > 1, 1).when(F.col(total_population_column).isNotNull(), 1),
    )

    # TODO part 4: check if they are the same across cis_area_code_20 by sample groups (by sample_source)

    return df
