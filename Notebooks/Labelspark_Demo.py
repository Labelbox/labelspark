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

# !pip install Labelbox ##uncomment if not installed 
# !pip install pillow 
# !pip install koalas 

try: API_KEY
except NameError: 
  API_KEY = dbutils.notebook.run("api_key", 60)

from labelbox import Client
client = Client(API_KEY)

# COMMAND ----------

# DBTITLE 1,Check Successful API Connection w/ Labelbox SDK 
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

tblList = spark.catalog.listTables()
if len(tblList) == 0: create_unstructured_dataset()
  
table_exists = False 
for table in tblList: 
    if table.name == "unstructured_data": 
      print("Unstructured data table exists")
      table_exists = True

if table_exists == False: create_unstructured_dataset()

# COMMAND ----------

# MAGIC %md 
# MAGIC ##Load Unstructured Data in Databricks##

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
for index, row in unstructured_data.iterrows():
  data_row_urls = [
    {
      "external_id" : row['external_id'],
      "row_data": row['row_data'] 
    }
  ]
  #note that we can easily send a batch of rows to Labelbox. Using a simple row-by-row creation for this demo. 
  dataSet_new.create_data_rows(data_row_urls)


# COMMAND ----------

# DBTITLE 1,Programmatically Set Up Ontology from Databricks
project = client.create_project(name = "Labelspark")
ontology = """
{
    "tools": [
        {
            "required": false,
            "name": "Segmentation",
            "tool": "superpixel",
            "color": "#1CE6FF",
            "classifications": []
        },
        {
            "required": false,
            "name": "BBox",
            "tool": "rectangle",
            "color": "#FF34FF",
            "classifications": [
                {
                    "required": false,
                    "instructions": "Nested Question",
                    "name": "nested_question",
                    "type": "radio",
                    "options": [
                        {
                            "label": "Option 1",
                            "value": "option_1"
                        },
                        {
                            "label": "Option 2",
                            "value": "option_2_"
                        }
                    ]
                }
            ]
        }
    ],
    "classifications": [
        {
            "required": false,
            "instructions": "Global Classifcation",
            "name": "global_classifcation",
            "type": "radio",
            "options": [
                {
                    "label": "Option 1",
                    "value": "option_1_"
                },
                {
                    "label": "Option 2",
                    "value": "option_2"
                }
            ]
        }
    ]
}
"""
# Connect Project 
project.datasets.connect(dataSet_new)

# Setup frontends 
all_frontends = list(client.get_labeling_frontends())
for frontend in all_frontends:
    if frontend.name == 'Editor':
        project_frontend = frontend
        break

# Attach Frontends
project.labeling_frontend.connect(project_frontend)

# Attach Project 
project.setup(project_frontend, ontology)

print("Project Setup is complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Bronze and Silver Annotation Tables##

# COMMAND ----------

def parse_export(export_file):
    bronze_new_json = []
    images = []
    
    for x in export_file: 
        # Delete unneeded features 
        x['Label'] = str(x['Label'])
        x['Benchmark ID'] = str(x['Benchmark ID'])
        x['Reviews'] = str(x['Reviews'])
        
         # Add values to List
        bronze_new_json.append(x)
        
    export_bronze = pd2.DataFrame(bronze_new_json)
    df_bronze = spark.createDataFrame(export_bronze)
    df_bronze.printSchema()
    df_bronze.registerTempTable("movie_stills_demo")
    display(df_bronze)
    
if __name__ == '__main__':
    client = Client(API_KEY) #refresh client 
    project = client.get_project("ckmvgzksjdp2b0789rqam8pnt")
    with urllib.request.urlopen(project.export_labels()) as url:
        export_file = json.loads(url.read().decode())
    parse_export(export_file)

# COMMAND ----------

# DBTITLE 1,Refine to Silver Table
def parse_export(export_file):
    new_json = []
    images = []
    for x in export_file: 
        if 'classifications' in x['Label']:
            count = 0
            for y in x['Label']['classifications']:
                answer = x['Label']['classifications'][count]['answer']['title']
                title = y['title']
                count = count + 1
                x[title] = answer
        
        # Get Image specs's
        url = x['Labeled Data']
        image = Image.open(urllib.request.urlopen(url))
        width, height = image.size
        # Add to JSON 
        x['Width'] = width
        x['Height'] = height 
        
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
        
    export = pd2.DataFrame(new_json)
    df = spark.createDataFrame(export)
    df.printSchema()
    
    df.registerTempTable("movie_stills_demo")
    display(df)

if __name__ == '__main__':
    client = Client(API_KEY) #refresh client 
    project = client.get_project("ckmvgzksjdp2b0789rqam8pnt")
    with urllib.request.urlopen(project.export_labels()) as url:
        export_file = json.loads(url.read().decode())
    parse_export(export_file)


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




# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC SELECT * FROM movie_stills_demo WHERE `Are there people in this still?` = "Yes"

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM football_stills_demo 
# MAGIC WHERE Quarterback IS NOT NULL 
# MAGIC and `Wide Receiver` IS NOT NULL
# MAGIC and `Tight End` IS NOT NULL
# MAGIC and `Running Back` IS NOT NULL

# COMMAND ----------


