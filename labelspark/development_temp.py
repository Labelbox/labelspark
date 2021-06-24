# Databricks notebook source
import json
import urllib
import databricks.koalas as pd
import ast
from labelbox import Client

#spark specific stuff
from pyspark.sql.types import StructType
from pyspark.sql.functions import col
from pyspark.sql import Row


# upload spark dataframe to Labelbox
def create_dataset(client, spark_dataframe, dataset_name="Default"):
    # expects spark dataframe to have two columns: external_id, row_data
    # external_id is the asset name ex: "photo.jpg"
    # row_data is the URL to the asset
    spark_dataframe = spark_dataframe.to_koalas()
    dataSet_new = client.create_dataset(name=dataset_name)

    # ported Pandas code to koalas
    data_row_urls = [{
        "external_id": row['external_id'],
        "row_data": row['row_data']
    } for index, row in spark_dataframe.iterrows()]
    upload_task = dataSet_new.create_data_rows(data_row_urls)
    upload_task.wait_till_done()
    print("Dataset created in Labelbox.")
    return dataSet_new


# returns raw bronze annotations
def get_annotations(client, project_id, spark, sc):
    project = client.get_project(project_id)
    with urllib.request.urlopen(project.export_labels()) as url:
        api_response_string = url.read().decode()  # this is a string of JSONs
        print(api_response_string)
    bronze_table = jsonToDataFrame(api_response_string, spark, sc)
    bronze_table = dataframe_schema_enrichment(bronze_table)
    return bronze_table


# processes the bronze table into a flattened one
def flatten_bronze_table(bronze_table):
    schema_fields_array = spark_schema_to_string(
        bronze_table.schema.jsonValue())  #generator of column names
    # Note that you cannot easily access some nested fields if you must navigate arrays of arrays to get there,
    # so I do try/except to avoid parsing into those fields.
    # I believe this can be enriched with some JSON parsing, but maybe another day.
    valid_schemas = []
    for schema_field in schema_fields_array:
        success = None
        try:
            success = bronze_table.select(col(schema_field))
            if success is not None:
                valid_schemas.append(schema_field)
        except Exception as e:
            # print(e.__class__, "occurred for", schema_field, "as it is probably inside an array of JSONs")
            schema_field_up_one_level = ".".join(
                schema_field.split(".")[:-1]
            )  # very complicated way of popping the last hierarchy level
            try:
                bronze_table.select(col(schema_field_up_one_level))
                if schema_field_up_one_level not in valid_schemas:
                    valid_schemas.append(schema_field_up_one_level)
            except Exception as e:
                pass  # print(e.__class__, "occurred for", schema_field, "so I'm skipping it")

    bronze_table = bronze_table.select(*valid_schemas).toDF(*valid_schemas)

    return bronze_table


# processes bronze table into silver table
def bronze_to_silver(bronze_table):
    # valid_schemas = list(spark_schema_to_string(bronze_table.schema.jsonValue()))
    bronze_table = flatten_bronze_table(bronze_table)

    bronze_table = bronze_table.withColumnRenamed("DataRow ID", "DataRowID")
    bronze_table = bronze_table.to_koalas()

    new_json = []
    for index, row in bronze_table.iterrows():
        my_dictionary = {}

        # classifications
        try:  #this won't work if there are no classifications
            for index, title in enumerate(row["Label.classifications.title"]):
                if "Label.classifications.answer" in row:
                    answer = row["Label.classifications.answer"][index]
                else:
                    answer = row["Label.classifications.answer.title"][index]
                my_dictionary = add_json_answers_to_dictionary(
                    title, answer, my_dictionary)
        except Exception as e:
            print("No classifications found.")

        # object counting
        try:  #this field won't work if the Label does not have objects in it
            for object in row.get("Label.objects.title", []):
                object_name = '{}.count'.format(object)
                if object_name not in my_dictionary:
                    my_dictionary[object_name] = 1  #initialize with 1
                else:
                    my_dictionary[object_name] += 1  #add 1 to counter
        except Exception as e:
            print("No objects found.")

        my_dictionary["DataRowID"] = row.DataRowID  # close it out
        new_json.append(my_dictionary)

    parsed_classifications = pd.DataFrame(new_json).to_spark()

    bronze_table = bronze_table.to_spark()
    joined_df = parsed_classifications.join(bronze_table, ["DataRowID"],
                                            "inner")
    joined_df = joined_df.withColumnRenamed("DataRowID", "DataRow ID")

    return joined_df  # silver_table


def jsonToDataFrame(json, spark, sc, schema=None):
    # code taken from Databricks tutorial https://docs.azuredatabricks.net/_static/notebooks/transform-complex-data-types-python.html
    reader = spark.read
    if schema:
        reader.schema(schema)
    return reader.json(sc.parallelize([json]))


# this LB-specific method converts schema to proper format based on common field names in our JSON

LABELBOX_DEFAULT_TYPE_DICTIONARY = {
    'Agreement': 'integer',
    'Benchmark Agreement': 'integer',
    'Created At': 'timestamp',
    'Updated At': 'timestamp',
    'Has Open Issues': 'integer',
    'Seconds to Label': 'float',
}


def dataframe_schema_enrichment(raw_dataframe, type_dictionary=None):
    if type_dictionary is None:
        type_dictionary = LABELBOX_DEFAULT_TYPE_DICTIONARY
    copy_dataframe = raw_dataframe
    for column_name in type_dictionary:
        try:
            copy_dataframe = copy_dataframe.withColumn(
                column_name,
                col(column_name).cast(type_dictionary[column_name]))
        except Exception as e:
            print(e.__class__, "occurred for", column_name, ":",
                  type_dictionary[column_name], ". Please check that column.")

    return copy_dataframe


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


def is_json(myjson):
    try:
        json_object = json.loads(myjson)
    except Exception as e:
        return False
    return True


def add_json_answers_to_dictionary(title, answer, my_dictionary):
    try:  # see if I can read the answer string as a literal --it might be an array of JSONs
        convert_from_literal_string = ast.literal_eval(answer)
        if isinstance(convert_from_literal_string, list):
            for item in convert_from_literal_string:  # should handle multiple items
                my_dictionary = add_json_answers_to_dictionary(
                    item["title"], item, my_dictionary)
            # recursive call to get into the array of arrays
    except Exception as e:
        pass

    if is_json(
            answer
    ):  # sometimes the answer is a JSON string; this happens when you have nested classifications
        parsed_answer = json.loads(answer)
        try:
            answer = parsed_answer[
                "value"]  # Labelbox syntax where the title is actually the "value" of the answer
        except Exception as e:
            pass

    my_dictionary[title] = answer

    return my_dictionary


# COMMAND ----------

from labelbox import Client
import databricks.koalas as pd
import labelspark

try: API_KEY
except NameError: 
  API_KEY = dbutils.notebook.run("api_key", 60)

# COMMAND ----------

client = Client(API_KEY)

projects = client.get_projects()
for project in projects:
    print(project.name, project.uid)

# COMMAND ----------

# returns raw bronze annotations
def get_annotations_video(client, project_id, spark, sc):
    project = client.get_project(project_id)
    with urllib.request.urlopen(project.export_labels()) as url:
        api_response_string = url.read().decode()  # this is a string of JSONs
        print(api_response_string)
    bronze_table = jsonToDataFrame(api_response_string, spark, sc)
    bronze_table = dataframe_schema_enrichment(bronze_table)
    return bronze_table

# COMMAND ----------

bronze_video_basic = get_annotations_video(client, "ckmvh3yb8a8dv0722mjsqcnzv", spark, sc)
display(bronze_video_basic)

# COMMAND ----------

import functools 

def unionAll(dfs):
    return functools.reduce(lambda df1,df2: df1.unionByName(df2.select(df1.columns)), dfs) 



# COMMAND ----------


def build_larger_dataframe(array_of_string_responses): 
  array_of_dfs = [] 
  for frame in array_of_string_responses: 
    df = jsonToDataFrame(frame, spark, sc)
    array_of_dfs.append(df)
  
  unioned_df = unionAll(array_of_dfs)
  return unioned_df 
  

# COMMAND ----------

from functools import reduce
from pyspark.sql import DataFrame

dfs = [df1,df2,df3]
df = reduce(DataFrame.unionAll, dfs)

# COMMAND ----------

from functools import reduce
from pyspark.sql import DataFrame
import requests 
koalas_bronze = bronze_video_basic.to_koalas()

headers = {'Authorization': f"Bearer {API_KEY}"}
master_array_of_json_arrays = [] 
for index, row in koalas_bronze.iterrows():
  print(row.Label.frames)
  response = requests.get(row.Label.frames, headers=headers, stream=False)
  array_of_string_responses = ["\"Label\":" + line.decode('utf-8') for line in response.iter_lines()]
  #array_of_jsons = [json.loads(line.decode('utf-8')) for line in response.iter_lines()]
  master_array_of_json_arrays.append(array_of_string_responses)
  
  #display(jsonToDataFrame(array_of_string_responses[0], spark, sc))

  #df = pd.DataFrame.from_dict(array_of_jsons[0], orient='index')
df = build_larger_dataframe(master_array_of_json_arrays[0][0:50])
display(df)
  
  
#   df = df.transpose()
#   display(df)
#   print(pd.DataFrame(array_of_jsons[0]))


# COMMAND ----------

display(bronze_to_silver(bronze_video_basic))

# COMMAND ----------

import json
import os
import requests
import json
import re

"""
The goal of this script is to help Labelbox users parse through the Video Export. The method is generating a JSON export
from project and using an API key. Insert the exported JSON file, and add the API key into the varibale 
API_KEY.  

Once this is completed the script will run. The script will save each label and its contents to a JSON file. The name 
of the JSON file will be the External ID of the file uploaded. This will save all Labels made to the documents folder. 

Email jvega@labelbox.com if you have any questions. 

"""

API_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySWQiOiJjanZnejMzOHEyanV1MDg2NjF6NGFwNWhnIiwib3JnYW5pemF0aW9uSWQiOiJjanZnejMzODAyanh5MGEwODM2cHNrMXg4IiwiYXBpS2V5SWQiOiJja2wwNHVmcWVmZ3I4MDc2MHVrb3FhOGw1IiwiaWF0IjoxNjEzMDAzODQ5LCJleHAiOjIyNDQxNTU4NDl9.4l838T53l4QRvf0U1q26u2K7wb8yqDENlh9DPcKJ6nk'

# Lists for parsing information
labels = []
label_frames = []
external_id = []

# Insert JSON export into this line
with open("/Users/jvega/Downloads/export-2021-02-11T00_35_33.480Z.json") as fp:
    labels = json.load(fp)

# Parse through all Labels to find Labels with contents and skipped
for x in labels:
    # Find Label frame URL's and External ID's
    if 'frames' in x['Label']:
        label_frames.append(x["Label"]["frames"])
        external_id.append(x['External ID'])
    else:
        # Print all skipped Labels
        print('Skipped Labels ID:' + x['ID'] + ", External ID: " + x['External ID'])

# Parse through label frames and external Ids in asynchronous fashion
for frame,file_path in zip(label_frames, external_id):
    # Attach API key to headers
    headers = {'Authorization': f"Bearer {API_KEY}"}
    # Pull response out of URL
    response = requests.get(frame, headers=headers, stream=False)
    # Open file with External ID name
    file_for_label = open(file_path + '.json', 'w')
    # Get contents of frames URl and save to file
    for frame_line in response.iter_lines():
        # if there is contents
        if frame_line:
            # Save frame to file set as external ID
            frame = json.loads(frame_line.decode('utf-8'))
            # Remove Schema and Feature ID's
            frame = re.sub(r'(\'featureId\'|\'schemaId\'): \'\w*\',', '',str(frame))
            # For NDJSON file
            file_for_label.write("%s\n" % frame)
            print(file_path)

