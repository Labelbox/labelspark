# Databricks notebook source
#this assumes you have another notebook called api_key and returns variable api_key 
api_key = dbutils.notebook.run("api_key", 60)

# COMMAND ----------

from labelbox import Client

if __name__ == '__main__':
    API_KEY = api_key
    client = Client(API_KEY)

# COMMAND ----------

projects = client.get_projects()
for project in projects:
    print(project.name, project.uid)

# COMMAND ----------


