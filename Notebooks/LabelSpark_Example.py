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
  dataSet = client.get_dataset("ckolyi9ha7h800y7i5ppr3put")

  #creates a list of datarow dictionaries 
  df_list = [ {
          "external_id": dataRow.external_id,
          "row_data": dataRow.row_data
      } for dataRow in dataSet.data_rows()]

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

if not table_exists: create_unstructured_dataset()

# COMMAND ----------

# MAGIC %md 
# MAGIC ##Load Unstructured Data##

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC select * from unstructured_data

# COMMAND ----------

# DBTITLE 1,Create Labelbox Client 
from labelbox import Client
client = Client(API_KEY)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC LabelSpark expects a spark table with two columns; the first column "external_id" and second column "row_data"
# MAGIC 
# MAGIC external_id is a filename, like "birds.jpg" or "my_video.mp4"
# MAGIC 
# MAGIC row_data is the URL path to the file. Labelbox renders assets locally on your users' machines when they label, so your labeler will need permission to access that asset. 
# MAGIC 
# MAGIC Example: 
# MAGIC 
# MAGIC | external_id | row_data                             |
# MAGIC |-------------|--------------------------------------|
# MAGIC | image1.jpg  | https://url_to_your_asset/image1.jpg |
# MAGIC | image2.jpg  | https://url_to_your_asset/image2.jpg |
# MAGIC | image3.jpg  | https://url_to_your_asset/image3.jpg |

# COMMAND ----------

# DBTITLE 1,Create Dataset with Labelbox for Annotation
import labelspark
unstructured_data = spark.table("unstructured_data")
dataSet_new = labelspark.create_dataset(client, unstructured_data, "Demo Dataset")

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC You can use the labelbox SDK to build your ontology. An example is provided below. 
# MAGIC 
# MAGIC Please refer to documentation at https://docs.labelbox.com/python-sdk/en/index-en

# COMMAND ----------

# DBTITLE 1,Set Up Your Ontology with OntologyBuilder 
from labelbox.schema.ontology import OntologyBuilder, Tool, Classification, Option
# from labelbox import Client
# import os

ontology = OntologyBuilder()
tool_people = Tool(tool=Tool.Type.BBOX, name="People")
tool_car = Tool(tool=Tool.Type.SEGMENTATION, name="Car")
tool_umbrella = Tool(tool=Tool.Type.POLYGON, name="Umbrella")
Weather_Classification = Classification(class_type=Classification.Type.RADIO, instructions="Weather", 
                                       options=[Option(value="Clear"), 
                                                Option(value="Overcast"),
                                                Option(value="Rain"),
                                                Option(value="Other")])
Time_of_Day = Classification(class_type=Classification.Type.RADIO, instructions="Time of Day", 
                                       options=[Option(value="Day"),
                                                Option(value="Night"),
                                                Option(value="Unknown")])

ontology.add_tool(tool_people)
ontology.add_tool(tool_car)
ontology.add_tool(tool_umbrella)
ontology.add_classification(Weather_Classification)
ontology.add_classification(Time_of_Day)


project_demo2 = client.create_project(name="LabelSpark Demo Example", description = "Example description here.")
project_demo2.datasets.connect(dataSet_new)

# Setup frontends 
all_frontends = list(client.get_labeling_frontends())
for frontend in all_frontends:
    if frontend.name == 'Editor':
        project_frontend = frontend
        break

# Attach Frontends
project_demo2.labeling_frontend.connect(project_frontend) 
# Attach Project and Ontology
project_demo2.setup(project_frontend, ontology.asdict()) 


print("Project Setup is complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Bronze and Silver Annotation Tables##

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC Be sure to provide your Labelbox Project ID (a long string like "ckolzeshr7zsy0736w0usbxdy") to labelspark get_annotations method to pull in your labeled dataset. 
# MAGIC 
# MAGIC <br>bronze_table = labelspark.get_annotations(client,"ckolzeshr7zsy0736w0usbxdy", spark, sc) 
# MAGIC 
# MAGIC *These other methods transform the bronze table and do not require a project ID.* 
# MAGIC <br>flattened_bronze_table = labelspark.flatten_bronze_table(bronze_table)
# MAGIC <br>silver_table = labelspark.bronze_to_silver(bronze_table)

# COMMAND ----------

# DBTITLE 1,Query Labelbox for Raw Annotations (Bronze Table)
client = Client(API_KEY) #refresh client 
bronze_table = labelspark.get_annotations(client,"ckolzeshr7zsy0736w0usbxdj", spark, sc) 
bronze_table.registerTempTable("street_photo_demo")
display(bronze_table.limit(2))

# COMMAND ----------

# DBTITLE 1,Bronze Table II (Labels Flattened ) 
client = Client(API_KEY) #refresh client 
bronze_table = spark.table("street_photo_demo")
flattened_bronze_table = labelspark.flatten_bronze_table(bronze_table)
display(flattened_bronze_table.limit(1))

# COMMAND ----------

# DBTITLE 1,Silver Table
client = Client(API_KEY) #refresh client 
silver_table = labelspark.bronze_to_silver(bronze_table)
silver_table.registerTempTable("silver_table")
display(silver_table)

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC SELECT * FROM silver_table 
# MAGIC WHERE `People.count` > 0 
# MAGIC AND `Umbrella.count` > 0
# MAGIC AND `Car.count` > 0
# MAGIC AND Weather = "Rain"

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC SELECT * FROM silver_table
# MAGIC WHERE `People.count` > 10

# COMMAND ----------

# DBTITLE 1,Demo Cleanup Code: Deleting Dataset and Projects
def cleanup(): 
  client = Client(API_KEY)
  dataSet_new.delete()
  project_demo2.delete()

cleanup() 
