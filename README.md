# Labelbox Connector for Apache Spark

Access the Labelbox Connector for Apache Spark to connect an unstructured dataset to Labelbox, programmatically set up an ontology for labeling, and return the labeled dataset in a Spark DataFrame. This library was designed to run in a Databricks environment, although it will function in any Spark environment with some modification. 

Labelbox is the enterprise-grade training data solution with fast AI enabled labeling tools, labeling automation, human workforce, data management, a powerful API for integration & SDK for extensibility. Visit [Labelbox](http://labelbox.com/) for more information.

This library is currently in beta. It may contain errors or inaccuracies and may not function as well as commercially released software. Please report any issues/bugs via [Github Issues](https://github.com/Labelbox/LabelSpark/issues).


## Table of Contents

* [Requirements](#requirements)
* [Installation](#installation)
* [Documentation](#documentation)
* [Authentication](#authentication)
* [Contribution](#contribution)

## Requirements

* Databricks Runtime 7.3 LTS or Later
* [Labelbox account](http://app.labelbox.com/)
* [Generate a Labelbox API key](https://labelbox.com/docs/api/getting-started#create_api_key)

## Installation

Install LabelSpark to your cluster by uploading a Python Wheel to the cluster, or via notebook-scoped library installation in the notebook. The installation will also add the Labelbox SDK, a requirement for LabelSpark to function. LabelSpark is available via pypi: 

```
pip install labelspark
```

## Documentation

Please consult the demo notebook in the "Notebooks" directory. LabelSpark includes 4 methods to help facilitate your workflow between Databricks and Labelbox. 

1. Create your dataset in Labelbox from Databricks: 

```
LB_dataset = labelspark.create_dataset(labelbox_client, spark_dataframe, "Name of Dataset")
```
Where "spark_dataframe" is your dataframe of unstructured data with asset names and asset URLs in two columns, named "external_id" and "row_data" respectively. 

| external_id | row_data                             |
|-------------|--------------------------------------|
| image1.jpg  | https://url_to_your_asset/image1.jpg |
| image2.jpg  | https://url_to_your_asset/image2.jpg |
| image3.jpg  | https://url_to_your_asset/image3.jpg |

2. Pull your raw annotations back into Databricks. 
```
bronze_DF = labelspark.get_annotations(client,"labelbox_project_id_here", spark, sc) 
```

3. You can use the our flattener to flatten the "Label" JSON column into component columns, or use the silver table method to produce a more queryable table of your labeled assets. Both of these methods take in the bronze table of annotations from above: 

```
flattened_bronze_DF = labelspark.flatten_bronze_table(bronze_DF)
queryable_silver_DF = labelspark.bronze_to_silver(bronze_DF)
```

### How To Get Video Project Annotations

Because Labelbox Video projects can contain multiple videos, you must use the `get_videoframe_annotations` method to return an array of DataFrames for each video in your project. Each DataFrame contains frame-by-frame annotation for a video in the project: 

```
bronze_video = labelspark.get_annotations(client,"labelbox_video_project_id_here", spark, sc) 
video_dataframes = labelspark.get_videoframe_annotations(bronze_video, API_KEY, spark, sc)    #note this extra step for video projects 
```
You may use standard LabelSpark methods iteratively to create your flattened bronze tables and silver tables: 
```
flattened_bronze_video_dataframes = []
silver_video_dataframes = [] 
for frameset in video_dataframes: 
  flattened_bronze_video_dataframes.append(labelspark.flatten_bronze_table(frameset))
  silver_video_dataframes.append(labelspark.bronze_to_silver(frameset))
```
This is how you would display the first video's frames and annotations, in sorted order: 
```
display(silver_video_dataframes[0]
        .join(bronze_video, ["DataRow ID"], "inner")
        .orderBy('frameNumber'), ascending = False)
```

While using LabelSpark, you will likely also use the Labelbox SDK (e.g. for programmatic ontology creation). These resources will help familiarize you with the Labelbox Python SDK: 
* [Visit our docs](https://labelbox.com/docs/python-api) to learn how the SDK works
* Checkout our [notebook examples](https://github.com/Labelbox/labelspark/tree/master/notebooks) to follow along with interactive tutorials
* view our [API reference](https://labelbox.com/docs/python-api/api-reference).

## Authentication

Labelbox uses API keys to validate requests. You can create and manage API keys on [Labelbox](https://app.labelbox.com/account/api-keys). We recommend using the Databricks Secrets API to store your key. If you don't have the Secrets API, you can store your API key in a separate notebook ignored by version control. 


## Contribution
Please consult `CONTRIB.md`


