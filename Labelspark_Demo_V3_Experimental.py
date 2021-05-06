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
from pyspark.sql.types import StructType    
from pyspark.sql.functions import col

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

if __name__ == '__main__':
    client = Client(API_KEY) #refresh client 
    #project = client.get_project("ckoaqvlhkcyqs0846kkfxtrqm")
    #project = client.get_project("ckoamhn1k5clr08584thrrp37") 
    #project = client.get_project("ckoaqvlhkcyqs0846kkfxtrqm") #inception 
    project = client.get_project("ckoc4obk6lxxa0784w1mr016x")
    with urllib.request.urlopen(project.export_labels()) as url:
        api_response_string = url.read().decode() #this is a string of JSONs 
    
    #testing json schema
    new_schema = None #StructType.fromJson(json.loads(json_schema))
    
    bronze_table = jsonToDataFrame(api_response_string, schema = new_schema)
    bronze_table = dataframe_schema_enrichment(bronze_table)
    bronze_table.registerTempTable("movie_stills_demo")
    
    display(bronze_table)
    

    
    #print(bronze_table.schema.json())
        

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Bronze Table II (Labels Flattened ) 
from pyspark.sql.functions import col

if __name__ == '__main__':
  
    client = Client(API_KEY) #refresh client 
    #project = client.get_project("ckmvgzksjdp2b0789rqam8pnt")
    bronze_table = spark.table("movie_stills_demo")
    
    schema_fields_array =  list(spark_schema_to_string(bronze_table.schema.jsonValue()))
    print(schema_fields_array)

    #Note that you cannot easily access some nested fields if you must navigate arrays of arrays to get there, so I do try/except to avoid parsing into those fields. I believe this can be enriched with some JSON parsing, but maybe another day. 
    valid_schemas = [] 
    for schema_field in schema_fields_array: 
      success = None
      try: 
        success = bronze_table.select(col(schema_field))
        if success is not None: 
          valid_schemas.append(schema_field)
      except Exception as e: 
        print(e.__class__, "occurred for", schema_field, "as it is probably inside an array of JSONs")
        schema_field_up_one_level = ".".join(schema_field.split(".")[:-1]) #very complicated way of popping the last hierarchy level 
        try: 
          bronze_table.select(col(schema_field_up_one_level))
          if schema_field_up_one_level not in valid_schemas: 
            valid_schemas.append(schema_field_up_one_level) 
        except Exception as e: print(e.__class__, "occurred for", schema_field, "so I'm skipping it")
    
    bronze_table2 = bronze_table.select(*valid_schemas).toDF(*valid_schemas)
    display(bronze_table2) #the ultimate bronze table 

# COMMAND ----------

# DBTITLE 1,Silver Table Experimental
from pyspark.sql import Row
import ast

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
  print(answer2)
  try:
    convert_from_literal_string = ast.literal_eval(answer2)
    print(type(convert_from_literal_string))
    if isinstance(convert_from_literal_string, list): 
      for item in convert_from_literal_string: #should handle multiple items
        print(item)
        my_dictionary = add_json_answers_to_dictionary(item["title"], item, my_dictionary) #recursive call 
  except Exception as e: 
    pass 
  print(answer2)
  if is_json(answer2): #sometimes the answer is a JSON string; this happens on project ckoamhn1k5clr08584thrrp37
    print("answer is json")
    parsed_answer = json.loads(answer2)
    print(parsed_answer)
    try: 
      my_dictionary[title] = parsed_answer["value"] #funky Labelbox syntax where the title is actually the "value" of the answer
    except Exception as e: 
      pass#my_dictionary[title] = parsed_answer #just dump it in the dataframe and fix it later 
#   elif isinstance(answer2, dict):
#     my_dictionary[title] = answer2["title"]
  else: 
    my_dictionary[title] = answer2
    
  return my_dictionary 

def bronze_to_silver(bronze_table): 
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
        try: #this is for deeper nesting 
          answer = row["Label.classifications.answer"][i]
        except Exception as e: 
          answer = row["Label.classifications.answer.title"][i]
        my_dictionary = add_json_answers_to_dictionary(title, answer, my_dictionary)
        #my_dictionary[title] = answer 
    except Exception as e: 
      print("not here")
    #objects
    array_of_objects = [] 
    
    try: 
      array_of_objects = row["Label.objects.classifications"]
    except Exception as e: 
      pass 
    
    for i in range(len(array_of_objects)):
      if array_of_objects[i] is not None: 
        for j in range(len(array_of_objects[i])): #there might be multiple objects in the list of objects
          title = array_of_objects[i][j].title #get the first row in the array 
          answer = array_of_objects[i][j].answer
          my_dictionary = add_json_answers_to_dictionary(title, answer, my_dictionary)
      
    my_dictionary["DataRowID"] = row.DataRowID #close it out 
    new_json.append(my_dictionary)
  
  parsed_classifications = pd.DataFrame(new_json).to_spark() #this is all leveraging Spark + Koalas! 

  return parsed_classifications #silver_table 
    

if __name__ == '__main__':
    client = Client(API_KEY) #refresh client 
    #project = client.get_project("ckmvgzksjdp2b0789rqam8pnt")
    #bronze_table = spark.table("movie_stills_demo")
    silver_table = bronze_to_silver(bronze_table2)
    #silver_table.registerTempTable("movie_stills_demo_silver")
    display(silver_table)
      

# COMMAND ----------

# DBTITLE 1,Silver Table V2
from pyspark.sql import Row

def is_json(myjson):
  try:
    json_object = json.loads(myjson)
  except ValueError as e:
    return False
  return True

def get_title_get_answer(classification_object, my_dictionary = {}, past_titles=[]):
  #print(classification_object.answer)
  title = None
  answer = None
  if type(classification_object.answer) is list: 
    print("I'm not sure if this code block is necessary unless you have multiple nested classes at same level")

  elif isinstance(classification_object.answer,Row): 
    answer = classification_object.answer.title #weird syntax 
    title = classification_object.title 
    my_dictionary[title] = answer
  else:
    answer = classification_object.answer
    
    #I had half written this code to consider nested recursive situations, but it is not needed because our JSON seems to flatten everything anyway 
#     if len(past_titles) > 0: 
#       past_titles_string = '-'.join(past_titles)
#       title = past_titles_string + classification_object.title
#     else: 
    title = classification_object.title
    
    if is_json(answer): #sometimes the answer is a JSON string; this happens on project ckoamhn1k5clr08584thrrp37
      print("answer is json for some reason")
      parsed_answer = json.loads(answer)
      print(parsed_answer)
      my_dictionary[title] = parsed_answer['value'] #funky Labelbox syntax where the title is actually the "value" of the answer 
    else: 
      my_dictionary[title] = answer
  
  return my_dictionary 

#TO-DO
def get_objects(objects_object, my_dictionary = {}, past_titles=[]): 
  return my_dictionary

def bronze_to_silver(bronze_table): 
  labels_only = bronze_table.select("DataRow ID","Label").withColumnRenamed("DataRow ID", "DataRowID")
  labels_and_objects = labels_only.select("DataRowID","Label.*")
  labels_and_objects = labels_and_objects.to_koalas()
     
  new_json = []
  for index, row in labels_and_objects.iterrows():
    my_dictionary = {}
    print(row.classifications)
    for classification in row.classifications:
      my_dictionary = get_title_get_answer(classification, my_dictionary)    
    for objects in row.objects: 
      my_dictionary = get_objects(objects, my_dictionary)
    
    my_dictionary["DataRowID"] = row.DataRowID #close it out 
    
    new_json.append(my_dictionary)
  
  parsed_classifications = pd.DataFrame(new_json).to_spark() #this is all leveraging Spark + Koalas! 
  bronze_table_simpler = bronze_table.withColumnRenamed("DataRow ID", "DataRowID").select("DataRowID", "Dataset Name", "External ID", "Labeled Data")
  silver_table = bronze_table_simpler.join(parsed_classifications, ["DataRowID"] ,"inner")
  silver_table = silver_table.withColumnRenamed("DataRowID", "DataRow ID")
  
  return silver_table 
    

if __name__ == '__main__':
    client = Client(API_KEY) #refresh client 
    #project = client.get_project("ckmvgzksjdp2b0789rqam8pnt")
    bronze_table = spark.table("movie_stills_demo")
    silver_table = bronze_to_silver(bronze_table)
    silver_table.registerTempTable("movie_stills_demo_silver")
    display(silver_table)
      

# COMMAND ----------

# DBTITLE 1,Refine to Silver Table 
def is_json(myjson):
  try:
    json_object = json.loads(myjson)
  except ValueError as e:
    return False
  return True

def get_title_get_answer_classifications(classification_object, my_dictionary = {}, past_titles=[]):
  #print(classification_object.answer)
  title = None
  answer = None
  if type(classification_object.answer) is list: 
    print("test")
#     for item in classification_object.answer: 
#       print("this ran")
#       title, answer = get_title_get_answer_classifications(item, my_dictionary, past_titles.append(item.title))
  #elif type(classification_object.answer) is Row()
  else:
    answer = classification_object.answer
    print(type(answer))
    past_titles_string = ""
    
    if len(past_titles) > 0: 
      past_titles_string = '-'.join(past_titles)
      title = past_titles_string + classification_object.title
    else: 
      title = classification_object.title
    
    if is_json(answer):
      parsed_answer = json.loads(answer)
      print(parsed_answer)
      my_dictionary[title] = parsed_answer['value'] #Labelbox syntax 
    else: 
      my_dictionary[title] = answer
  
  return my_dictionary 



def bronze_to_silver(bronze_table): 
  labels_only = bronze_table.select("DataRow ID","Label").withColumnRenamed("DataRow ID", "DataRowID")
  labels_and_objects = labels_only.select("DataRowID","Label.*")
  labels_and_objects = labels_and_objects.to_koalas()
     
  new_json = []
  for index, row in labels_and_objects.iterrows():
    my_dictionary = {}
    for classification in row.classifications:
      #print(classification)
#       answer = classification.answer.title 
#       title = classification.title
#       my_dictionary[title] = answer
      #print(classification)
      my_dictionary = get_title_get_answer(classification, my_dictionary)
      my_dictionary["DataRowID"] = row.DataRowID
    new_json.append(my_dictionary)
  
  print(new_json)
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


