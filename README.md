# The Official Labelbox <> Databricks Python Integration

[Labelbox](https://labelbox.com/) enables teams to maximize the value of their unstructured data with its enterprise-grade training data platform. For ML use cases, Labelbox has tools to deploy labelers to annotate data at massive scale, diagnose model performance to prioritize labeling, and plug in existing ML models to speed up labeling. For non-ML use cases, Labelbox has a powerful catalog with auto-computed similarity scores that users can leverage to label large amounts of data with a couple clicks.

This library was designed to run in a Databricks environment, although it will function in any Spark environment with some modification.

We strongly encourage collaboration - please free to fork this repo and tweak the code base to work for you own data, and make pull requests if you have suggestions on how to enhance the overall experience, add new features, or improve general performance. 

Please report any issues/bugs via [Github Issues](https://github.com/Labelbox/labelspark/issues).

## Table of Contents

* [Requirements](#requirements)
* [Setup](#setup)
* [Example Notebooks](#example-notebooks)

## Requirements

* Databricks: Runtime 10.4 LTS or Later
* Apache Spark: 3.1.2 or Later
* [Labelbox account](http://app.labelbox.com/)
* [Generate a Labelbox API key](https://docs.labelbox.com/reference/create-api-key)

## Setup

Set up LabelSpark with the following lines of code:

```
%pip install labelspark -q
import labelspark as ls

api_key = "" # Insert your Labelbox API key here
client = ls.Client(api_key)
```

Once set up, you can run the following core functions:

- `client.create_data_rows_from_table()` :   Creates Labelbox data rows (and metadata) given a Spark Table DataFrame

- `client.export_to_table()` :  Exports labels (and metadata) from a given Labelbox project and creates a Spark DataFrame

## Example Notebooks

### Importing Data

|            Notebook            |  Github  |
| ------------------------------ | -------- |
| Basics: Data Rows from URLs            | [![Github](https://img.shields.io/badge/GitHub-100000?logo=github&logoColor=white)](notebooks/intro.ipynb)  | 
| Data Rows with Metadata        | [![Github](https://img.shields.io/badge/GitHub-100000?logo=github&logoColor=white)](notebooks/metadata.ipynb)  | 
| Data Rows with Attachments     | [![Github](https://img.shields.io/badge/GitHub-100000?logo=github&logoColor=white)](notebooks/attachments.ipynb)  | 
| Data Rows with Annotations     | [![Github](https://img.shields.io/badge/GitHub-100000?logo=github&logoColor=white)](notebooks/annotations.ipynb)  | 
| Putting it all Together        | [![Github](https://img.shields.io/badge/GitHub-100000?logo=github&logoColor=white)](notebooks/full-demo.ipynb)  | 
------

### Exporting Data

|            Notebook            |  Github  |
| ------------------------------ | -------- |
| Exporting Data to a Spark Table            | [![Github](https://img.shields.io/badge/GitHub-100000?logo=github&logoColor=white)](notebooks/export.ipynb)  |
------

While using LabelSpark, you will likely also use the Labelbox SDK (e.g. for programmatic ontology creation). These resources will help familiarize you with the Labelbox Python SDK: 
* [Visit our docs](https://docs.labelbox.com/reference/install-python-sdk) to learn how the SDK works
* Checkout our [notebook examples](https://github.com/Labelbox/labelspark/tree/master/notebooks) to follow along with interactive tutorials
* View the Labelbox [API reference](https://labelbox-python.readthedocs.io/en/latest/).
