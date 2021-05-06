# Databricks notebook source
# MAGIC %md
# MAGIC ##Notebook Setup##

# COMMAND ----------

# DBTITLE 0,Project Setup
import os
import json
import re
import urllib
import requests
import databricks.koalas as pd
import pandas as pd2
import os
from PIL import Image
from labelbox import Client

# !pip install Labelbox ##uncomment if not installed 
# !pip install pillow 
# !pip install koalas 

try: API_KEY
except NameError: 
  API_KEY = dbutils.notebook.run("api_key", 60)


# COMMAND ----------

# DBTITLE 1,Check Successful API Connection w/ Labelbox SDK 
client = Client(API_KEY)

projects = client.get_projects()
for project in projects:
    print(project.name, project.uid)

# COMMAND ----------

# DBTITLE 1,Load demo table of images and URLs
# can parse the directory and make a Spark table of image URLs

def create_unstructured_dataset(): 
  print("Creating table of unstructured image data")
  # Pull information from Data Lake or other storage  
  dataSet = client.get_dataset("ckmu2e5yi7ttd0709mi4qgnwd")
  df_list = []
  for dataRow in dataSet.data_rows():
      df_ = {
          "external_id": dataRow.external_id,
          "row_data": dataRow.row_data
      }
      df_list.append(df_)

  # Create DataFrame 
  images = pd.DataFrame(df_list)
  df_images = images.to_spark()
#   display(df_images)
  df_images.registerTempTable("unstructured_data")
  # df_images = spark.createDataFrame(images) 

table_exists = False 
tblList = spark.catalog.listTables()
if len(tblList) == 0: 
  create_unstructured_dataset()
  table_exists = True

for table in tblList: 
    if table.name == "unstructured_data": 
      print("Unstructured data table exists")
      table_exists = True

if table_exists == False: create_unstructured_dataset()

# COMMAND ----------

import labelspark 
labelspark.this_is_a_method()

# COMMAND ----------

# MAGIC %md 
# MAGIC ##Load Unstructured Data##

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC select * from unstructured_data

# COMMAND ----------

# DBTITLE 1,Create Dataset with Labelbox for Annotation
# Pass image URLs to Labelbox for annotation 

def create_dataset_from_spark(client, spark_dataframe, dataset_name = "Default"): 
  #expects spark dataframe to have two columns: external_id, row_data
  #external_id is the asset name ex: "photo.jpg"
  #row_data is the URL to the asset 
  spark_dataframe = spark_dataframe.to_koalas()
  dataSet_new = client.create_dataset(name = dataset_name)
  
  dataRow_json = []
  #ported Pandas code to koalas
  data_row_urls = [
      {
        "external_id" : row['external_id'],
        "row_data": row['row_data'] 
      } for index, row in 
      spark_dataframe.iterrows()
  ]
  upload_task = dataSet_new.create_data_rows(data_row_urls)
  upload_task.wait_till_done()
  print("Dataset created in Labelbox.")
  return dataSet_new

if __name__ == '__main__':
  unstructured_data = spark.table("unstructured_data")
  dataSet_new = create_dataset_from_spark(client, unstructured_data, "My Sample Dataset")
  

# COMMAND ----------

# DBTITLE 1,Set Up Your Ontology with OntologyBuilder 
from labelbox.schema.ontology import OntologyBuilder, Tool, Classification, Option
from labelbox import Client
from getpass import getpass
import os

ontology = OntologyBuilder(
    tools=[
        Tool(tool=Tool.Type.BBOX, name="Sample Box"),
        Tool(tool=Tool.Type.SEGMENTATION,
             name="Sample Segmentation",
             classifications=[
                 Classification(class_type=Classification.Type.TEXT,
                                instructions="name")
             ])
    ],
    classifications=[
        Classification(class_type=Classification.Type.RADIO,
                       instructions="Sample Radio Button",
                       options=[Option(value="Option A"),
                                Option(value="Option B")])
    ])

# print(ontology.asdict())

project_demo2 = client.create_project(name="LabelSpark", description = "Your project description goes here")

# Connect Project to dataset 
project_demo2.datasets.connect(dataSet_new)

# Setup frontends 
all_frontends = list(client.get_labeling_frontends())
for frontend in all_frontends:
    if frontend.name == 'Editor':
        project_frontend = frontend
        break

# Attach Frontends
project_demo2.labeling_frontend.connect(project_frontend) #how does this work?? 

# Attach Project and Ontology
project_demo2.setup(project_frontend, ontology.asdict())

print("Project Setup is complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Bronze and Silver Annotation Tables##

# COMMAND ----------

#imports 
from pyspark.sql.types import StructType    
from pyspark.sql.functions import col
from pyspark.sql import Row
import ast

# COMMAND ----------

# DBTITLE 1,Query Labelbox for Raw Annotations (Bronze Table)
 def jsonToDataFrame(json, schema=None):
  #code taken from Databricks tutorial https://docs.azuredatabricks.net/_static/notebooks/transform-complex-data-types-python.html
  reader = spark.read
  if schema:
    reader.schema(schema)
  return reader.json(sc.parallelize([json]))

#this LB-specific method converts schema to proper format based on common field names in our JSON 
def dataframe_schema_enrichment(raw_dataframe, type_dictionary = None):
  if type_dictionary is None: 
    type_dictionary = {
      'Agreement':'integer',
      'Benchmark Agreement': 'integer',
      'Created At': 'timestamp',
      'Updated At': 'timestamp',
      'Has Open Issues': 'integer',
      'Seconds to Label': 'float', 
                        }
  copy_dataframe = raw_dataframe
  for column_name in type_dictionary:
    try: 
      copy_dataframe = copy_dataframe.withColumn(column_name, col(column_name).cast(type_dictionary[column_name]))
    except Exception as e: print(e.__class__, "occurred for", column_name,":",type_dictionary[column_name],". Moving to next item.")
  return copy_dataframe

def get_annotations(client, project_id): 
  project = client.get_project(project_id)
  with urllib.request.urlopen(project.export_labels()) as url:
    api_response_string = url.read().decode() #this is a string of JSONs 
  
  bronze_table = jsonToDataFrame(api_response_string)
  bronze_table = dataframe_schema_enrichment(bronze_table)
  return bronze_table 

if __name__ == '__main__':
    client = Client(API_KEY) #refresh client 
    bronze_table = get_annotations(client,"ckoc9avswe2tc08469c5sne5w")
    display(bronze_table)
    bronze_table.registerTempTable("movie_stills_demo")
    
    #example of object and classification in same project: ckoc9avswe2tc08469c5sne5w
    #project = client.get_project("ckoaqvlhkcyqs0846kkfxtrqm")
    #project = client.get_project("ckoamhn1k5clr08584thrrp37") 
    #project = client.get_project("ckoaqvlhkcyqs0846kkfxtrqm") #inception 
    #project = client.get_project("ckoc4obk6lxxa0784w1mr016x")
#     with urllib.request.urlopen(project.export_labels()) as url:
#         api_response_string = url.read().decode() #this is a string of JSONs 

#     new_schema = None #StructType.fromJson(json.loads(json_schema))
    
#     bronze_table = jsonToDataFrame(api_response_string, schema = new_schema)
#     bronze_table = dataframe_schema_enrichment(bronze_table)
#     bronze_table.registerTempTable("movie_stills_demo")
    
    

    
    #print(bronze_table.schema.json())
        

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Bronze Table II (Labels Flattened ) 

#helper code from https://pwsiegel.github.io/tech/nested-spark/
def spark_schema_to_string(schema, progress=''):
    if schema['type'] == 'struct':
        for field in schema['fields']:
            key = field['name']
            yield from spark_schema_to_string(field, f'{progress}.{key}')
    elif schema['type'] == 'array':
        if type(schema['elementType']) == dict:
            yield from spark_schema_to_string(schema['elementType'], progress)
        else:
            yield progress.strip('.')
    elif type(schema['type']) == dict:
        yield from spark_schema_to_string(schema['type'], progress)
    else:
        yield progress.strip('.')
        
def flatten_bronze_table(bronze_table): 
  schema_fields_array =  list(spark_schema_to_string(bronze_table.schema.jsonValue()))
    #Note that you cannot easily access some nested fields if you must navigate arrays of arrays to get there, so I do try/except to avoid parsing into those fields. I believe this can be enriched with some JSON parsing, but maybe another day. 
  valid_schemas = []
  for schema_field in schema_fields_array:
    success = None
    try:
      success = bronze_table.select(col(schema_field))
      if success is not None: 
        valid_schemas.append(schema_field)
    except Exception as e:
      #print(e.__class__, "occurred for", schema_field, "as it is probably inside an array of JSONs")
      schema_field_up_one_level = ".".join(schema_field.split(".")[:-1]) #very complicated way of popping the last hierarchy level 
      try:
        bronze_table.select(col(schema_field_up_one_level))
        if schema_field_up_one_level not in valid_schemas:
          valid_schemas.append(schema_field_up_one_level) 
      except Exception as e: pass #print(e.__class__, "occurred for", schema_field, "so I'm skipping it")
        
  bronze_table = bronze_table.select(*valid_schemas).toDF(*valid_schemas) 
  
  return bronze_table 

if __name__ == '__main__':
  
    client = Client(API_KEY) #refresh client 
    bronze_table = spark.table("movie_stills_demo")
    flattened_bronze_table = flatten_bronze_table(bronze_table)
    display(flattened_bronze_table)


# COMMAND ----------

# DBTITLE 1,Silver Table
def is_json(myjson):
  try:
    json_object = json.loads(myjson)
  except Exception as e:
    return False
  return True

def is_array_string(mystring): #a very quick and dirty way to see if this is an array string of JSON
  if mystring[0] == "[" and mystring[-1] == "]": 
    return True
  else: return False 
  
def add_json_answers_to_dictionary(title, answer2, my_dictionary): 
  try: #see if I can read the answer string as a literal --it might be an array of JSONs
    convert_from_literal_string = ast.literal_eval(answer2)
    if isinstance(convert_from_literal_string, list): 
      for item in convert_from_literal_string: #should handle multiple items
        my_dictionary = add_json_answers_to_dictionary(item["title"], item, my_dictionary) 
        #recursive call to get into the array of arrays
  except Exception as e: 
    pass 
  
  if is_json(answer2): #sometimes the answer is a JSON string; this happens on project ckoamhn1k5clr08584thrrp37
    parsed_answer = json.loads(answer2)
    try: 
      my_dictionary[title] = parsed_answer["value"] #funky Labelbox syntax where the title is actually the "value" of the answer
    except Exception as e: 
      pass
  else: 
    my_dictionary[title] = answer2
    
  return my_dictionary 

def bronze_to_silver(bronze_table):
  #valid_schemas = list(spark_schema_to_string(bronze_table.schema.jsonValue()))
  bronze_table = flatten_bronze_table(bronze_table)
  
  bronze_table = bronze_table.withColumnRenamed("DataRow ID", "DataRowID") 
  bronze_table = bronze_table.to_koalas()
  
  new_json = []
  for index, row in bronze_table.iterrows():
    my_dictionary = {}
    
    #classifications
    try: 
      row["Label.classifications.title"]
      for i in range(len(row["Label.classifications.title"])): 
        title = row["Label.classifications.title"][i]
        try: #this is for deeper nesting; sometimes this fails if there isn't more nesting, hence try except
          answer = row["Label.classifications.answer"][i]
        except Exception as e: 
          answer = row["Label.classifications.answer.title"][i]
        my_dictionary = add_json_answers_to_dictionary(title, answer, my_dictionary)
    except Exception as e: 
      print("No classifications") 
    
    my_dictionary["DataRowID"] = row.DataRowID #close it out 
    new_json.append(my_dictionary)
  
  parsed_classifications = pd.DataFrame(new_json).to_spark() #this is all leveraging Spark + Koalas! 
  
  bronze_table = bronze_table.to_spark()
  joined_df = parsed_classifications.join(bronze_table, ["DataRowID"], "inner")
  joined_df = joined_df.withColumnRenamed("DataRowID", "DataRow ID")
  
  return joined_df #silver_table 
    
if __name__ == '__main__':
    client = Client(API_KEY) #refresh client 
    silver_table = bronze_to_silver(bronze_table)
    display(silver_table)
      

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC SELECT * FROM movie_stills_demo_silver WHERE `Are there people in this still?` = "Yes"

# COMMAND ----------

# DBTITLE 1,Demo Cleanup Code: Deleting Dataset and Projects
client = Client(API_KEY)
dataSet_new.delete()
project_demo2.delete()
