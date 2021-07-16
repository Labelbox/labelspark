# Databricks notebook source
#We recommend using Databricks Secrets API to create a variable for your Labelbox API Key, but if you do not have access to the Secrets API you can use this notebook template to store your API key in a separate notebook. Be sure to include in gitignore to avoid committing your API key to Git.

api_key = "insert api key"

dbutils.notebook.exit(
    api_key)  #returns api_key if you call this notebook via a notebook workflow

###example code for notebook workflow w/ dbutils will get api_key from notebook "api_key"
# try: API_KEY
# except NameError:
#   API_KEY = dbutils.notebook.run("api_key", 60)
