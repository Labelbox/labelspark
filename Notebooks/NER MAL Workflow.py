# Databricks notebook source
import sys
sys.path.append('./')

from labelbox import Client
from labelbox.schema.ontology import OntologyBuilder, Tool
from labelbox import Project, Dataset, Client, LabelingFrontend
from typing import Dict, Any
!pip install flair
from flair.models import SequenceTagger
from flair.data import Sentence
!pip install wikipedia-api
import wikipediaapi
import uuid
import requests
import ndjson
import os
from getpass import getpass

# COMMAND ----------

try: API_KEY
except NameError: 
  API_KEY = dbutils.notebook.run("api_key", 60)

from labelbox import Client
client = Client(API_KEY)

# COMMAND ----------

ENDPOINT = "https://api.labelbox.com/graphql"

# COMMAND ----------

client = Client(api_key=API_KEY, endpoint=ENDPOINT)

# COMMAND ----------

ontology_builder = OntologyBuilder(tools=[
    Tool(tool=Tool.Type.NER, name="org"),
    Tool(tool=Tool.Type.NER, name="person"),
])

# COMMAND ----------

wiki_wiki = wikipediaapi.Wikipedia('en')
page_py = wiki_wiki.page('Marketing')
project = client.create_project(name="ner_mal_project")
dataset = client.create_dataset(name="net_mal_dataset")
data_row = dataset.create_data_row(row_data=page_py.text)
editor = next(
    client.get_labeling_frontends(where=LabelingFrontend.name == 'editor'))
project.setup(editor, ontology_builder.asdict())
project.datasets.connect(dataset)

# COMMAND ----------

project.enable_model_assisted_labeling()

# COMMAND ----------

ontology = OntologyBuilder.from_project(project)
feature_schema_lookup = {
    tool.name: tool.feature_schema_id for tool in ontology.tools
}

# COMMAND ----------

#Create model
model = SequenceTagger.load('ner-ontonotes-fast')
sentence = Sentence(page_py.text)
model.predict(sentence)
entities = sentence.to_dict(tag_type='ner')['entities']

# COMMAND ----------

def get_ner_ndjson(datarow_id: str, feature_schema_id: str, start: int,
                   end: int) -> Dict[str, Any]:
    """
    * https://docs.labelbox.com/data-model/en/index-en#entity
    
    Args:
        datarow_id (str): id of the data_row (in this case image) to add this annotation to
        feature_schema_id (str): id of the bbox tool in the current ontology
        start (int): Character index where the entity begins
        end (int): Character index where the entity ends
    Returns:
        ndjson representation of a named entity
    """
    return {
        "uuid": str(uuid.uuid4()),
        "schemaId": feature_schema_id,
        "dataRow": {
            "id": datarow_id
        },
        "location": {
            "start": start,
            "end": end
        }
    }

# COMMAND ----------

ndjsons = []
for entity in entities:
    pred = entity["labels"][0].value.lower()
    if pred not in ["org", "person"]:
        continue

    start = entity["start_pos"]
    end = entity["end_pos"]
    ndjsons.append(
        get_ner_ndjson(data_row.uid, feature_schema_lookup[pred], start, end))

# COMMAND ----------

upload_task = project.upload_annotations(name=f"upload-job-{uuid.uuid4()}",
                                         annotations=ndjsons,
                                         validate=True)

# COMMAND ----------

#Wait for upload to finish (Will take up to five minutes)
upload_task.wait_until_done()
#Review the upload status
for status in upload_task.statuses[:3]:
    print(status)

# COMMAND ----------


