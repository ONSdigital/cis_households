import json
from collections import defaultdict

from cishouseholds.pyspark_utils import get_or_create_spark_session
from phm.lookup import lookup
from phm.lookup import phm_validation_schema

# Open a json file into a json object
# Note that F.read() returns the content
# of the file as a string so we should call
# json.loads()


def decode_phm_json(json_dict: dict):
    table = json_dict["submission"]
    meta = table.pop("survey_metadata")
    data = table.pop("data")
    answers = defaultdict(lambda: list())
    list_items = defaultdict(lambda: list())

    # process answer data into single nested degree dictionary keyed by PHM answer codes
    for i, answer in enumerate(data["answers"]):
        if answer.get("list_item_id"):
            list_items[answer["list_item_id"]] = answer["answer_id"]

        if isinstance(answer["value"], dict):  # this isnt used rn
            for k, v in answer["value"].items():
                answers[data["answer_codes"][k]["code"]] = v
        else:
            answers[data["answer_codes"][i]["code"]] = answer["value"]

    answers.update(meta)
    answers.update({k: v for k, v in table.items() if not isinstance(v, (list, dict))})

    # update keys from lookup
    answers = {lookup.get(k, k): v for k, v in answers.items()}  # type: ignore
    return answers, list_items


with open("C:/code/cis_households/phm/json_example.json", encoding="utf-8") as F:
    json_data = json.loads(F.read())
    answers, list_items = decode_phm_json(json_data)

test = {k: v for k, v in answers.items() if not isinstance(v, list)}
df = get_or_create_spark_session().createDataFrame(data=[tuple(test.values())], schema=phm_validation_schema)
