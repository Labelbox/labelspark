# Databricks notebook source
# MAGIC %md
# MAGIC # Labelbox Connector for Databricks Tutorial Notebook

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pre-requisites
# MAGIC 1. This tutorial notebook requires a Lablbox API Key. Please login to your [Labelbox Account](app.labelbox.com) and generate an [API Key](https://app.labelbox.com/account/api-keys)
# MAGIC 2. A few cells below will install the Labelbox SDK and Connector Library. This install is notebook-scoped and will not affect the rest of your cluster. 
# MAGIC 3. Please make sure you are running at least the latest LTS version of Databricks. 
# MAGIC 
# MAGIC #### Notebook Preview
# MAGIC This notebook will guide you through these steps: 
# MAGIC 1. Connect to Labelbox via the SDK 
# MAGIC 2. Create a labeling dataset from a table of unstructured data in Databricks
# MAGIC 3. Programmatically set up an ontology and labeling project in Labelbox
# MAGIC 4. Load Bronze and Silver annotation tables from an example labeled project 
# MAGIC 5. Additional cells describe how to handle video annotations and use Labelbox Diagnostics and Catalog 
# MAGIC 
# MAGIC Additional documentation links are provided at the end of the notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC Thanks for trying out the Databricks and Labelbox Connector! Unless you downloaded this directly from our repository, you or someone from your organization signed up for a Labelbox trial through Databricks Partner Connect. This notebook helps illustrate how Labelbox and Databricks can be used together to power unstructured data workflows. 
# MAGIC 
# MAGIC Labelbox can be used to rapidly annotate a variety of unstructured data from your Data Lake ([images](https://labelbox.com/product/image), [video](https://labelbox.com/product/video), [text](https://labelbox.com/product/text), and [geospatial tiled imagery](https://docs.labelbox.com/docs/tiled-imagery-editor)) and the Labelbox Connector for Databricks makes it easy to bring the annotations back into your Lakehouse environment for AI/ML and analytical workflows. 
# MAGIC 
# MAGIC If you would like to watch a video of the workflow, check out our [Data & AI Summit Demo](https://databricks.com/session_na21/productionizing-unstructured-data-for-ai-and-analytics). 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC <img src="https://labelbox.com/static/images/partnerships/collab-chart.svg" alt="example-workflow" width="800"/>
# MAGIC 
# MAGIC <b>Questions or comments? Reach out to us at [hello+databricks@labelbox.com](mailto:hello+databricks@labelbox.com)
# MAGIC   
# MAGIC Last Updated: July 29, 2022

# COMMAND ----------

# DBTITLE 1,Install Labelbox Library & Labelbox Connector for Databricks
# MAGIC %pip install git+https://github.com/Labelbox/labelspark.git@rj_create_dataset_update#egg=labelspark&subdirectory=labelspark

# COMMAND ----------

#This will import Koalas or Pandas-on-Spark based on your DBR version. 
from pyspark import SparkContext
from packaging import version
sc = SparkContext.getOrCreate()
if version.parse(sc.version) < version.parse("3.2.0"):
  import databricks.koalas as pd 
  needs_koalas = True  
else:
  import pyspark.pandas as pd
  needs_koalas = False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure the SDK
# MAGIC 
# MAGIC Now that Labelbox and the Databricks libraries have been installed, you will need to configure the SDK. You will need an API key that you can create through the app [here](https://app.labelbox.com/account/api-keys). You can also store the key using Databricks Secrets API. The SDK will attempt to use the env var `LABELBOX_API_KEY`

# COMMAND ----------

from labelbox import Client, Dataset
from labelbox.schema.ontology import OntologyBuilder, Tool, Classification, Option

API_KEY = "" #Paste in your API key from Labelbox as a string, or use Databricks Secrets API 

if not(API_KEY):
  raise ValueError("Go to Labelbox to get an API key")
  
client = Client(API_KEY)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create seed data
# MAGIC 
# MAGIC Next we'll load a demo dataset into a Spark table so you can see how to easily load assets into Labelbox via URLs with the Labelbox Connector for Databricks. 
# MAGIC 
# MAGIC Also, Labelbox has native support for AWS, Azure, and GCP cloud storage. You can connect Labelbox to your storage via [Delegated Access](https://docs.labelbox.com/docs/iam-delegated-access) and easily load those assets for annotation. For more information, you can watch this [video](https://youtu.be/wlWo6EmPDV4).
# MAGIC 
# MAGIC You can also add data to Labelbox [using the Labelbox SDK directly](https://docs.labelbox.com/docs/datasets-datarows). We recommend using the SDK if you have complicated dataset creation requirements (e.g. including metadata with your dataset) which aren't handled by the Labelbox Connector for Databricks.

# COMMAND ----------

# DBTITLE 1,Initialize Sample DataFrame with Metadata
sample_dataset_dict_metadata = {
  "external_id":["sample1.jpg", "sample2.jpg", "sample3.jpg", "sample4.jpg", "sample5.jpg", 
                 "sample6.jpg", "sample7.jpg", "sample8.jpg", "sample9.jpg", "sample10.jpg"],
  "row_data":[
    "https://storage.googleapis.com/diagnostics-demo-data/coco/COCO_train2014_000000247422.jpg",
    "https://storage.googleapis.com/diagnostics-demo-data/coco/COCO_train2014_000000484849.jpg",
    "https://storage.googleapis.com/diagnostics-demo-data/coco/COCO_train2014_000000215782.jpg",
    "https://storage.googleapis.com/diagnostics-demo-data/coco/COCO_val2014_000000312024.jpg",
    "https://storage.googleapis.com/diagnostics-demo-data/coco/COCO_train2014_000000486139.jpg",
    "https://storage.googleapis.com/diagnostics-demo-data/coco/COCO_train2014_000000302713.jpg",
    "https://storage.googleapis.com/diagnostics-demo-data/coco/COCO_train2014_000000523272.jpg",
    "https://storage.googleapis.com/diagnostics-demo-data/coco/COCO_train2014_000000094514.jpg",
    "https://storage.googleapis.com/diagnostics-demo-data/coco/COCO_val2014_000000050578.jpg",
    "https://storage.googleapis.com/diagnostics-demo-data/coco/COCO_train2014_000000073727.jpg"],
  "short_sample": [15243524, 243154, 43252432, 2434534, 45243524, 4524352435, 5452, 25245452, 24452435, 244524354],  
  "int_sample": [1,2, 3, 4, 5, 6, 7, 0, 9, 10],
  "long_sample": [15243524, 243154, 43252432, 2434534, 45243524, 4524352435, 5452, 25245452, 24452435, 244524354],
  "float_sample": [1.2 ,2.5, 3.6, 4, 5.0, 6, None, None, 9, 10],  
  "double_sample": [1, 2, 3, 4, 5, 6, 7, None, 9, 10],    
  "decimal_sample": [1.2 ,2.5, 3.6, 4, 5.0, 6, 0, 24.3, 9, 10], 
  "enum_sample": ["A", "B", "C", "A", "B", "C", "A", "B", "C", "A"],
  "string_sample" : ["test1", "test2", "test3", "test4", "test5", "test6", "test7", "test8", "test9", "test10"],
  "boolean_sample": [True ,False, True, True, False, False, True, None, True, False],
  "timestamp_sample": ["2022-03-18T22:06:28.172Z", "2022-03-18T22:06:28.172Z", "2022-03-18T22:06:28.172Z", "2022-03-18T22:06:28.172Z", "2022-03-18T22:06:28.172Z", "2022-03-18T22:06:28.172Z", None, None, None, None],
  "date_sample": ["2022-07-25", "2022-07-23", "2022-07-21", "2022-07-19", "2022-07-05", "2022-07-25", "2022-07-25", "2022-07-25", "2022-07-25", "2022-07-25"]}

sample_spark_dataframe = pd.DataFrame.from_dict(sample_dataset_dict_metadata).to_spark() #produces our demo Spark table of datarows for Labelbox

# COMMAND ----------

# can parse the directory and make a Spark table of image URLs
SAMPLE_TABLE = "sample_unstructured_data"
tblList = spark.catalog.listTables()

if not any([table.name == SAMPLE_TABLE for table in tblList]):
  sample_spark_dataframe.createOrReplaceTempView(SAMPLE_TABLE)
  print(f"Registered table: {SAMPLE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC You should now have a temporary table "sample_unstructured_data" which includes the file names and URLs, and some metadata fields for our demo images. We're going to use this table with Labelbox using the Labelbox Connector for Databricks!

# COMMAND ----------

display(sqlContext.sql(f"select * from {SAMPLE_TABLE} LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a Labeling Project
# MAGIC 
# MAGIC Projects are where teams create labels. A project is requires a dataset of assets to be labeled and an ontology to configure the labeling interface.
# MAGIC 
# MAGIC ### Step 1: Create a dataset
# MAGIC 
# MAGIC The [Labelbox Connector for Databricks](https://pypi.org/project/labelspark/) expects a spark table with two columns; the first column "external_id" and second column "row_data". You can also include optional metadata columns by supplying a metadata_index dictionary in create_dataset().
# MAGIC 
# MAGIC external_id is a filename, like "birds.jpg" or "my_video.mp4"
# MAGIC 
# MAGIC row_data is the URL path to the file. Labelbox renders assets locally on your users' machines when they label, so your labeler will need permission to access that asset. 
# MAGIC 
# MAGIC Example: 
# MAGIC 
# MAGIC | external_id | row_data                             | optional_metadata_field | optional_metadata_field_2| ... |
# MAGIC |-------------|--------------------------------------|-------------------------|--------------------------|-----|
# MAGIC | image1.jpg  | https://url_to_your_asset/image1.jpg |  "Example string 1"     |          1234            | ... |
# MAGIC | image2.jpg  | https://url_to_your_asset/image2.jpg |  "Example string 2"     |          88.8            | ... |
# MAGIC | image3.jpg  | https://url_to_your_asset/image3.jpg |  "Example string 3"     |          123.5           | ... |

# COMMAND ----------

import labelspark

'''We create a dictionary to map column names of our Spark Table to the correct metadata format in Labelbox. Synatx is {key=column_name: value=metadata_type} Metadata type is one of the following strings:
  "enum"
  "string" 
  "number" 
  "datetime"
  
  Metadata is optional.
'''

metadata_labelbox_data_types = {
  'short_sample' : "string",
  'int_sample' : "number",
  'long_sample' : "string",
  'float_sample' : "number",
  'double_sample' : "number",
  'decimal_sample' : "number",
  'enum_sample' : "enum",
  'string_sample' : "string",
  'boolean_sample' : "enum",
  'timestamp_sample' : "datetime",
  'date_sample' : "datetime"
}

unstructured_data = spark.table(SAMPLE_TABLE) #load our sample table 

demo_dataset = labelspark.create_dataset(client, 
                                         sample_spark_dataframe, 
                                         dataset_name="Demo Dataset", 
                                         iam_integration='DEFAULT', 
                                         metadata_index = metadata_labelbox_data_types)

# COMMAND ----------

print("Open the dataset in the App")
print(f"https://app.labelbox.com/data/{demo_dataset.uid}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Create a project
# MAGIC 
# MAGIC You can use the labelbox SDK to build your ontology (we'll do that next) You can also set your project up entirely through our website at app.labelbox.com.
# MAGIC 
# MAGIC Check out our [ontology creation documentation.](https://docs.labelbox.com/docs/configure-ontology)

# COMMAND ----------

# Create a new project
project_demo = client.create_project(name="Labelbox and Databricks Example")
project_demo.datasets.connect(demo_dataset)  # add the dataset to the queue

ontology = OntologyBuilder()

tools = [
  Tool(tool=Tool.Type.BBOX, name="Car"),
  Tool(tool=Tool.Type.BBOX, name="Flower"),
  Tool(tool=Tool.Type.BBOX, name="Fruit"),
  Tool(tool=Tool.Type.BBOX, name="Plant"),
  Tool(tool=Tool.Type.SEGMENTATION, name="Bird"),
  Tool(tool=Tool.Type.SEGMENTATION, name="Person"),
  Tool(tool=Tool.Type.SEGMENTATION, name="Dog"),
  Tool(tool=Tool.Type.SEGMENTATION, name="Gemstone"),
]
for tool in tools: 
  ontology.add_tool(tool)

conditions = ["clear", "overcast", "rain", "other"]

weather_classification = Classification(
    class_type=Classification.Type.RADIO,
    instructions="what is the weather?", 
    options=[Option(value=c) for c in conditions]
)  
ontology.add_classification(weather_classification)

# Setup editor
for editor in client.get_labeling_frontends():
    if editor.name == 'Editor':
        project_demo.setup(editor, ontology.asdict()) 

print("Project Setup is complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Go label data

# COMMAND ----------

print("Open the project to start labeling")
print(f"https://app.labelbox.com/projects/{project_demo.uid}/overview")

# COMMAND ----------

raise ValueError("Go label some data before continuing")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Exporting labels/annotations
# MAGIC 
# MAGIC After creating labels in Labelbox you can export them to use in Databricks for model training and analysis.

# COMMAND ----------

LABEL_TABLE = "exported_labels"

# COMMAND ----------

labels_table = labelspark.get_annotations(client, project_demo.uid, spark, sc)
labels_table.createOrReplaceTempView(LABEL_TABLE)
display(labels_table)

# COMMAND ----------

display(labelspark.bronze_to_silver(labels_table))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Other features of Labelbox
# MAGIC 
# MAGIC [Model Assisted Labeling](https://docs.labelbox.com/docs/model-assisted-labeling)
# MAGIC <br>Once you train a model on your initial set of unstructured data, you can plug that model into Labelbox to support a Model Assisted Labeling workflow. Review the outputs of your model, make corrections, and retrain with ease! You can reduce future labeling costs by >50% by leveraging model assisted labeling.
# MAGIC 
# MAGIC <img src="https://files.readme.io/4c65e12-model-assisted-labeling.png" alt="MAL" width="800"/>
# MAGIC 
# MAGIC [Catalog](https://docs.labelbox.com/docs/catalog)
# MAGIC <br>Once you've created datasets and annotations in Labelbox, you can easily browse your datasets and curate new ones in Catalog. Use your model embeddings to find images by similarity search. 
# MAGIC 
# MAGIC <img src="https://files.readme.io/14f82d4-catalog-marketing.jpg" alt="Catalog" width="800"/>
# MAGIC 
# MAGIC [Model Diagnostics](https://labelbox.com/product/model-diagnostics)
# MAGIC <br>Labelbox complements your MLFlow experiment tracking with the ability to easily visualize experiment predictions at scale. Model Diagnostics helps you quickly identify areas where your model is weak so you can collect the right data and refine the next model iteration. 
# MAGIC 
# MAGIC <img src="https://images.ctfassets.net/j20krz61k3rk/4LfIELIjpN6cou4uoFptka/20cbdc38cc075b82f126c2c733fb7082/identify-patterns-in-your-model-behavior.png" alt="Diagnostics" width="800"/>

# COMMAND ----------

# DBTITLE 1,More Info
# MAGIC %md
# MAGIC While using the Labelbox Connector for Databricks, you will likely use the Labelbox SDK (e.g. for programmatic ontology creation). These resources will help familiarize you with the Labelbox Python SDK: 
# MAGIC * [Visit our docs](https://labelbox.com/docs/python-api) to learn how the SDK works
# MAGIC * Checkout our [notebook examples](https://github.com/Labelbox/labelspark/tree/master/notebooks) to follow along with interactive tutorials
# MAGIC * view our [API reference](https://labelbox.com/docs/python-api/api-reference).
# MAGIC 
# MAGIC <b>Questions or comments? Reach out to us at [hello+databricks@labelbox.com](mailto:hello+databricks@labelbox.com)

# COMMAND ----------

# MAGIC %md
# MAGIC Copyright Labelbox, Inc. 2022. The source in this notebook is provided subject to the [Labelbox Terms of Service](https://docs.labelbox.com/page/terms-of-service).  All included or referenced third party libraries are subject to the licenses set forth below.
# MAGIC 
# MAGIC |Library Name|Library license | Library License URL | Library Source URL |
# MAGIC |---|---|---|---|
# MAGIC |Labelbox Python SDK|Apache-2.0 License |https://github.com/Labelbox/labelbox-python/blob/develop/LICENSE|https://github.com/Labelbox/labelbox-python
# MAGIC |Labelbox Connector for Databricks|Apache-2.0 License |https://github.com/Labelbox/labelspark/blob/master/LICENSE|https://github.com/Labelbox/labelspark
# MAGIC |Python|Python Software Foundation (PSF) |https://github.com/python/cpython/blob/master/LICENSE|https://github.com/python/cpython|
# MAGIC |Apache Spark|Apache-2.0 License |https://github.com/apache/spark/blob/master/LICENSE|https://github.com/apache/spark|
