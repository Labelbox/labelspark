# Databricks notebook source
#this assumes you have another notebook called api_key and returns variable api_key 
api_key = dbutils.notebook.run("api_key", 60)
print(api_key)

# COMMAND ----------


