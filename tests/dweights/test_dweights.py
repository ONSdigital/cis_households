from cishouseholds.dweights_1167 import chose_scenario_of_dweight_for_antibody_different_household


def test_chose_scenario_of_dweight_for_antibody_different_household():

    unit_test_dict = {
        # not in list = scenario A
        1: {
            "tranche_eligible_indicator": "indicator_x",
            "household_samples_dataframe": [],
            "number_eligible_households_tranche_bystrata": None,
            "number_sampled_households_tranche_bystrata": None,
            "result": "A",
        },
        2: {
            "tranche_eligible_indicator": "indicator_x",
            "household_samples_dataframe": ["indicator_x"],
            "number_eligible_households_tranche_bystrata": 8,
            "number_sampled_households_tranche_bystrata": 8,
            "result": "B",
        },
        3: {
            "tranche_eligible_indicator": "indicator_x",
            "household_samples_dataframe": ["indicator_x"],
            "number_eligible_households_tranche_bystrata": 10,
            "number_sampled_households_tranche_bystrata": 3,
            "result": "C",
        },
    }

    for case in unit_test_dict.values():
        scenario = chose_scenario_of_dweight_for_antibody_different_household(
            tranche_eligible_indicator=case["tranche_eligible_indicator"],
            household_samples_dataframe=case["household_samples_dataframe"],
            n_eligible_hh_tranche_bystrata=case["number_eligible_households_tranche_bystrata"],
            n_sampled_hh_tranche_bystrata=case["number_sampled_households_tranche_bystrata"],
        )
        assert scenario == case["result"]
