# Databricks notebook source
#this assumes you have another notebook called api_key and returns variable api_key 
api_key = dbutils.notebook.run("api_key", 60)


# COMMAND ----------

# DBTITLE 1,Get Labelbox Client
from labelbox import Client

if __name__ == '__main__':
    API_KEY = api_key
    client = Client(API_KEY)

# COMMAND ----------

# DBTITLE 1,Check Successful API Connection w/ Labelbox SDK 
projects = client.get_projects()
for project in projects:
    print(project.name, project.uid)

# COMMAND ----------

# DBTITLE 1,Load directory of images and/or video
#can parse the directory and make a Spark table of image URLs
#https://docs.databricks.com/data/data-sources/aws/amazon-s3.html#access-s3-objects-as-local-files

# COMMAND ----------

# DBTITLE 1,Create Labelbox Project w/ all the datarows (this will work best w/ Delegated Access I think)
#Call LB SDK 

#Create Project

#Do a bulk add of the datarows to avoid rate limits





# COMMAND ----------

# DBTITLE 1,Call LB SDK and get back all the labeled assets, pull it into Databricks as Delta Table
#Bronze Table



# COMMAND ----------

#Silver Table 



# COMMAND ----------

#Rinse and repeat for Video --or can video be done at the same time as images??? 

# COMMAND ----------

# DBTITLE 1,QUERIES 
#Find me all frames with XYZ in it 

#Find me the scenes with a superhero 

#etc etc etc 
