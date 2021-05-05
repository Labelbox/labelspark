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

# MAGIC %md 
# MAGIC ##Load Unstructured Data##

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC select * from unstructured_data

# COMMAND ----------

# DBTITLE 1,Create Dataset with Labelbox for Annotation
# Pass image URLs to Labelbox for annotation 

unstructured_data = spark.table("unstructured_data")
unstructured_data = unstructured_data.to_koalas()

dataSet_new = client.create_dataset(name = "Sample DataSet LabelSpark")
dataRow_json = []

#ported Pandas code to koalas
data_row_urls = [
    {
      "external_id" : row['external_id'],
      "row_data": row['row_data'] 
    } for index, row in 
    unstructured_data.iterrows()
]
upload_task = dataSet_new.create_data_rows(data_row_urls)
upload_task.wait_till_done()


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
project_demo2.labeling_frontend.connect(project_frontend)

# Attach Project 
project_demo2.setup(project_frontend, ontology.asdict())

print("Project Setup is complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Bronze and Silver Annotation Tables##

# COMMAND ----------

# DBTITLE 1,Query Labelbox for Bronze Annotation Table
def jsonToDataFrame(json, schema=None):
  #code taken from Databricks tutorial https://docs.azuredatabricks.net/_static/notebooks/transform-complex-data-types-python.html
  reader = spark.read
  if schema:
    reader.schema(schema)
  return reader.json(sc.parallelize([json]))

if __name__ == '__main__':
    client = Client(API_KEY) #refresh client 
    project = client.get_project("ckoamhn1k5clr08584thrrp37") 
    with urllib.request.urlopen(project.export_labels()) as url:
        api_response_string = url.read().decode() #this is a string of JSONs 
    
    bronze_table = jsonToDataFrame(api_response_string, schema = None)
    bronze_table.registerTempTable("movie_stills_demo")
    
    display(bronze_table)
        

# COMMAND ----------

# DBTITLE 1,Refine to Silver Table 
def bronze_to_silver(bronze_table): 
  labels_only = bronze_table.select("DataRow ID","Label").withColumnRenamed("DataRow ID", "DataRowID")
  labels_and_objects = labels_only.select("DataRowID","Label.*")
  labels_and_objects = labels_and_objects.to_koalas()
     
  new_json = []
  for index, row in labels_and_objects.iterrows():
    my_dictionary = {}
    
    for classification in row.classifications:
      answer = classification.answer.title 
      title = classification.title
      my_dictionary[title] = answer
      my_dictionary["DataRowID"] = row.DataRowID
    new_json.append(my_dictionary)
    
  parsed_classifications = pd.DataFrame(new_json).to_spark() #this is all leveraging Spark + Koalas! 
  bronze_table_simpler = bronze_table.withColumnRenamed("DataRow ID", "DataRowID").select("DataRowID", "Dataset Name", "External ID", "Labeled Data")
  silver_table = bronze_table_simpler.join(parsed_classifications, ["DataRowID"] ,"inner")
    
  return silver_table 
    

if __name__ == '__main__':
    client = Client(API_KEY) #refresh client 
    #project = client.get_project("ckmvgzksjdp2b0789rqam8pnt")
    bronze_table = spark.table("movie_stills_demo")
    silver_table = bronze_to_silver(bronze_table)
    silver_table.registerTempTable("movie_stills_demo_silver")
    display(silver_table)
      

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from movie_stills_demo_silver

# COMMAND ----------

# DBTITLE 1,Example of Video Annotations
# Lists for parsing information
labels = []
label_frames = []
video_url = []
external_id = []
frames = []

# Export 
project = client.get_project("ckmvh3yb8a8dv0722mjsqcnzv")
with urllib.request.urlopen(project.export_labels()) as url:
  export_file = json.loads(url.read().decode())

# Parse through all Labels to find Labels with contents and skipped
for x in export_file:
    # Find Label frame URL's and External ID's
    if 'frames' in x['Label']:
        label_frames.append(x["Label"]["frames"])
        external_id.append(x['External ID'])
        video_url.append(x['Labeled Data'])
        

# Parse through label frames and external Ids in asynchronous fashion
for frame,file_path, video in zip(label_frames, external_id, video_url):
    # Attach API key to headers
    headers = {'Authorization': f"Bearer {API_KEY}"}
    # Pull response out of URL
    response = requests.get(frame, headers=headers, stream=False)
    # Get contents of frames URl and save to file
    for frame_line in response.iter_lines():
        # if there is contents
        if frame_line:
            # Save frame to file set as external ID
            frame = json.loads(frame_line.decode('utf-8'))
            # Remove Schema and Feature ID's
            frame = re.sub(r'(\'featureId\'|\'schemaId\'): \'\w*\',', '', str(frame))
            frame = frame.replace("\'", "\"")

            # Additions!
            frame = frame.replace("True", "\"True\"")
            frame = frame.replace("False", "\"False\"")
            frame = json.loads(frame)
            frame['Video URL'] = str(video)
            frame['External_ID'] = str(external_id)
            frames.append(frame)
count = 0 
video_json_file = []
for x in frames: 
  #List  frame 1 
  for y in x['classifications']:
      answer = y['answer']['title']
      title = y['title']
      x[title] = answer
      
  del x['classifications']
  del x['objects']
  video_json_file.append(x)
  
  
df_video = pd2.DataFrame(video_json_file)
df_video = spark.createDataFrame(df_video)
df_video.printSchema()
df_video.registerTempTable("video_frames_demo")
display(df_video)

# COMMAND ----------

# DBTITLE 1,Football Example
def parse_export(export_file):
    new_json = []
    images = []
    for x in export_file: 
#         print(x)
        if "objects" in x['Label']:
            count = 0
            for z in x['Label']['objects']:
                answer = x['Label']['objects'][count]['bbox']
                title = z['title']
                count = count + 1
                x[title] = str(answer)
        # Delete unneeded features 
        del x['Label']
        del x['Agreement']
        del x['Benchmark Agreement']
        del x['Benchmark ID']
        del x['Reviews']
        del x['Has Open Issues']
        del x['DataRow ID']
        del x['ID']
        # Add values to List
        new_json.append(x)
        
#         # Get Image specs's
#         url = x['Labeled Data']
#         image = Image.open(urllib.request.urlopen(url))
#         width, height = image.size
#         # Add to JSON 
#         x['Width'] = width
#         x['Height'] = height 
        
    export = pd2.DataFrame(new_json)
    df = spark.createDataFrame(export)
    df.registerTempTable("football_stills_demo")
    display(df)
    
    
    

if __name__ == '__main__':
    Chris_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySWQiOiJja2JpNmZheDNkdWx4MDcyMGk1MGNoanJoIiwib3JnYW5pemF0aW9uSWQiOiJja2JpNmZhd21lYTJ1MDc0MHY4eHo0ZjFoIiwiYXBpS2V5SWQiOiJja213Y3F3MW10NDJlMDc1N2FidjU4dzRiIiwiaWF0IjoxNjE3MTI4ODIwLCJleHAiOjIyNDgyODA4MjB9.Fmd2ETKT4vg2l5YZU5CCq31aT07kjrHqoCpiR8sKgsU"
    client = Client(Chris_key)
    project = client.get_project("ckf4r0tqd15sl0799v1u0pk5i")
    
    with urllib.request.urlopen(project.export_labels()) as url:
        export_file = json.loads(url.read().decode())
    parse_export(export_file)
    
    display()



# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC SELECT * FROM movie_stills_demo_silver WHERE `Are there people in this still?` = "Yes"

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM football_stills_demo 
# MAGIC WHERE Quarterback IS NOT NULL 
# MAGIC and `Wide Receiver` IS NOT NULL
# MAGIC and `Tight End` IS NOT NULL
# MAGIC and `Running Back` IS NOT NULL

# COMMAND ----------

# DBTITLE 1,Demo Cleanup Code: Deleting Dataset and Projects
client = Client(API_KEY)
dataSet_new.delete()
project_demo2.delete()

# COMMAND ----------


