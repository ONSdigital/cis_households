import json
from collections import defaultdict
from typing import List
from typing import Tuple
from typing import Union

from cishouseholds.phm.lookup import lookup
from cishouseholds.phm.lookup import phm_validation_schema

# Open a json file into a json object
# Note that F.read() returns the content
# of the file as a string so we should call
# json.loads()


def decode_phm_json(json_str: Union[str, bytes]) -> List[Tuple]:
    json_dict = json.loads(json_str)
    # table = json_dict["submission"]
    answers_list = []
    for table in json_dict.values():

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

        # add missing values
        answers = {k: answers.get(k) for k in phm_validation_schema.keys()}  # type: ignore
        answers_list.append(tuple(answers.values()))
    return answers_list
