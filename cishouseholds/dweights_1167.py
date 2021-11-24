# from pyspark.sql import DataFrame
# from pyspark.sql import functions as F
# from pyspark.sql.window import Window
from typing import Any
from typing import List
from typing import Tuple
from typing import Union


def chose_scenario_of_dweight_for_antibody_different_household(
    tranche_eligible_indicator: str,
    household_samples_dataframe: List[str],
    n_eligible_hh_tranche_bystrata,
    n_sampled_hh_tranche_bystrata,
) -> Union[str, None]:
    """
    Decide what scenario to use for calculation of the design weights
    for antibodies for different households
    Parameters
    ----------
    tranche_eligible_indicator
    household_samples_dataframe
    n_eligible_hh_tranche_bystrata
    n_sampled_hh_tranche_bystrata
    """

    if (n_eligible_hh_tranche_bystrata is None) and (n_sampled_hh_tranche_bystrata is None):
        eligibility_pct = 0

    elif ((n_eligible_hh_tranche_bystrata is not None) and (n_sampled_hh_tranche_bystrata is not None)) and (
        n_sampled_hh_tranche_bystrata > 0
    ):
        eligibility_pct = (
            100 * (n_eligible_hh_tranche_bystrata - n_sampled_hh_tranche_bystrata) / n_sampled_hh_tranche_bystrata
        )

    if tranche_eligible_indicator not in household_samples_dataframe:
        return "A"
    else:
        # TODO: if eligibility_pct < 0 ??
        if eligibility_pct == 0:
            return "B"
        elif eligibility_pct > 0:
            return "C"
        else:
            return None


# 1168
def raw_dweight_for_AB_scenario_for_antibody(
    hh_dweight_antibodies, raw_dweight_antibodies, sample_new_previous, scaled_dweight_swab_nonadjusted
) -> Tuple[Any, Any]:
    """
    Parameters
    ----------
    hh_dweight_antibodies
    raw_dweight_antibodies
    sample_new_previous
    scaled_dweight_swab_nonadjusted
    """
    # 1 step: copy the household_dweight_antibodies into the raw_dweight_antibodies
    # variable when they exist. Note:it should be always a value for previous cases
    raw_dweight_antibodies = raw_dweight_antibodies + hh_dweight_antibodies

    # when sample_new_previous = "previous", hh_dweight_antibodies not missing;
    # then  raw_dweight_antibodies_b = hh_dweight_antibodies
    if (sample_new_previous == "previous") and (hh_dweight_antibodies is not None):
        raw_dweight_antibodies_b = hh_dweight_antibodies

    # 2 step: take scale_dweight_swab_nonadjusted as raw_dweight_antibodies, when
    # hh_dweight_antibodies are missing (cis_area level)
    # Note: hh_dweight_antibodies values for the new cases will be always missing
    if hh_dweight_antibodies is None:
        raw_dweight_antibodies = scaled_dweight_swab_nonadjusted

        # when sample_new_previous= "new" ; hh_dweight_antibodies missing for same
        # cis_area_code_20,
        # raw_dweight_antibodies_b = scaled_dweight_swab_nonadjusted
        if sample_new_previous == "new":
            raw_dweight_antibodies_b = scaled_dweight_swab_nonadjusted

    return raw_dweight_antibodies_b, raw_dweight_antibodies_b
