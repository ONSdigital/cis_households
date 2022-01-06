from typing import Any
from typing import List

from pyspark.sql import column, functions as F
from pyspark.sql.dataframe import DataFrame


def operation1(country_check: Any,*args, **logic):
    print("logic: ",logic)
    return F.when(
            country_check,
            logic["if_country"],
        ).otherwise(logic["otherwise"]),


def operation2(country_check: Any, age_group_antibody_column, *args, **logic):
    return F.when(
            country_check & (F.col(age_group_antibody_column).isNull()),
            logic["first"],
        ).when(country_check, logic["second"]).otherwise(logic["otherwise"])

def operation3(condition, operation):
    return F.when(
            condition,
            operation,
    )

def operation4(column_name, **logic):
    return F.when(logic["math"], F.lit(1)).otherwise(F.col(column_name))


def wrapper(df: DataFrame, country_column: str, age_group_antibody_column:str):
    operations = {
        1:operation1,
        2:operation2
    }
    for column_name, logic in dictionary.items():
        inner_function = operations[logic["operation"]]
        if type(logic["country"]) != list:
            logic["country"] = [logic["country"]]
        df = df.withColumn(
            column_name, inner_function(F.col(country_column).isin(logic["country"]),age_group_antibody_column, **logic)
        )


dictionary = {
    "p1_for_swab_longcovid":{
        "operation": 1,
        "country": "England",
        "if_country": ((F.col("interim_region_code") -1) * 14) + (F.col("interim_sex") - 1) + F.col("age_group_swab"),
        "otherwise": ((F.col("interim_sex") - 1) * 7) + F.col("age_group_swab")
    },
    "p1_for_antibodies_evernever_engl":{
        "operation": 1,
        "country": "England",
        "if_country": ((F.col("interim_region_code") - 1) * 10) + ((F.col("interim_sex") - 1) *  5) + F.col("age_group_antibody"),
        "otherwise": None
    },
    "p1_for_antibodies_28daysto_engl":{
        "operation": 1,
        "country": "England",
        "if_country": ((F.col("interim_sex") - 1) * 5) + F.col("age_group_antibody"),
        "otherwise": None
    },
    "p1_for_antibodies_wales_scot_ni":{
        "operation": 1,
        "country": "England",
        "if_country": None,
        "otherwise": ((F.col("interim_sex") - 1) * 5) + F.col("age_group_antibody")  
    },
    "p3_for_antibodies_28daysto_engl":{
        "operation": 2,
        "country": "England",
        "logic1": F.col("interim_region_code"),
        "logic2": "missing",
        "logic_otherwise": ((F.col("interim_sex") - 1) * 5) + F.col("age_group_antibody")
    },
    # "p1_swab_longcovid_england":{
    #     "operation":3,
    #     "countries":,
    #     "datasets":[],

    # }
}