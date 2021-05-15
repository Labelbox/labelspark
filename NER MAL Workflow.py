# Databricks notebook source


# COMMAND ----------

import sys
sys.path.append('./')

from labelbox import Client
from labelbox.schema.ontology import OntologyBuilder, Tool
from labelbox import Project, Dataset, Client, LabelingFrontend
from typing import Dict, Any
from flair.models import SequenceTagger
from flair.data import Sentence
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


