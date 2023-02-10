import json
from collections import  defaultdict

# Open a json file into a json object
# Note that F.read() returns the content
# of the file as a string so we should call
# json.loads()
from cishouseholds.pyspark_utils import get_or_create_spark_session

with open('C:\code\cis_households\cishouseholds\json_example.json', encoding='utf-8') as F:
  json_data = json.loads(F.read())
data = json_data["submission"]["data"]
answers = defaultdict(lambda: list())
list_items = defaultdict(lambda :list())
for answer in data["answers"]:
  if answer.get("list_item_id"):
    list_items[answer.get("list_item_id")] = answer["answer_id"]

  if isinstance(answer["value"], dict):
    for k,v in answer["value"].items():
      answers[answer["answer_id"] + "_" + k] = v
  else:
    answers[answer["answer_id"]] = answer["value"]

for k,v in answers.items():
  print(k,v)

print(dict(list_items))
test = {k:v for k,v in answers.items() if not isinstance(v,list)}
df = get_or_create_spark_session().createDataFrame(data=[tuple(test.values())],schema=list(test.keys()))
df.show()
