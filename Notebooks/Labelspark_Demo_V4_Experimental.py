# Databricks notebook source
# MAGIC %md
# MAGIC ##Notebook Setup##

# COMMAND ----------

# DBTITLE 0,Project Setup
from labelbox import Client
import databricks.koalas as pd
import labelspark

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

# DBTITLE 1,Demo-Prep: Load demo table of images and URLs
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
unstructured_data = spark.table("unstructured_data")
dataSet_new = labelspark.create_dataset_from_spark(client, unstructured_data, "My Sample Dataset")

# COMMAND ----------

# DBTITLE 1,Set Up Your Ontology with OntologyBuilder 
from labelbox.schema.ontology import OntologyBuilder, Tool, Classification, Option
# from labelbox import Client
# import os

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

# DBTITLE 1,Query Labelbox for Raw Annotations (Bronze Table)
client = Client(API_KEY) #refresh client 
bronze_table = labelspark.get_annotations(client,"ckoc9avswe2tc08469c5sne5w", spark, sc)
bronze_table.registerTempTable("movie_stills_demo")
display(bronze_table)

# COMMAND ----------

# DBTITLE 1,Bronze Table II (Labels Flattened ) 
client = Client(API_KEY) #refresh client 
bronze_table = spark.table("movie_stills_demo")
flattened_bronze_table = labelspark.flatten_bronze_table(bronze_table)
display(flattened_bronze_table)


# COMMAND ----------

# DBTITLE 1,Silver Table
client = Client(API_KEY) #refresh client 
silver_table = labelspark.bronze_to_silver(bronze_table)
silver_table.registerTempTable("silver_table")
display(silver_table)

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC SELECT * FROM silver_table WHERE `Are there birds in this scene?` = "Yes"

# COMMAND ----------

# DBTITLE 1,Demo Cleanup Code: Deleting Dataset and Projects
client = Client(API_KEY)
dataSet_new.delete()
project_demo2.delete()
