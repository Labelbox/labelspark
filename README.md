# Labelbox Connector for Databricks

Access the Labelbox Connector for Databricks to connect an unstructured dataset to Labelbox, programmatically set up an ontology for labeling, and return the labeled dataset in a Spark DataFrame. This library was designed to run in a Databricks environment, although it will function in any Spark environment with some modification. 

Labelbox is the enterprise-grade training data solution with fast AI enabled labeling tools, labeling automation, human workforce, data management, a powerful API for integration & SDK for extensibility. Visit [Labelbox](http://labelbox.com/) for more information.

This library is currently in beta. It may contain errors or inaccuracies and may not function as well as commercially released software. Please report any issues/bugs via [Github Issues](https://github.com/Labelbox/LabelSpark/issues).


## Table of Contents

* [Requirements](#requirements)
* [Installation](#installation)
* [Documentation](#documentation)
* [Authentication](#authentication)
* [Contribution](#contribution)

## Requirements

* Databricks: Runtime 10.4 LTS or Later
* Apache Spark: 3.1.2 or Later
* [Labelbox account](http://app.labelbox.com/)
* [Generate a Labelbox API key](https://docs.labelbox.com/reference/create-api-key)

## Installation

Install LabelSpark to your cluster by uploading a Python Wheel to the cluster, or via notebook-scoped library installation in the notebook. The installation will also add the Labelbox SDK, a requirement for LabelSpark to function. LabelSpark is available via pypi: 

```
pip install labelspark
```

## Documentation

Please consult the demo notebook in the "Notebooks" directory. LabelSpark includes 4 methods to help facilitate your workflow between Databricks and Labelbox. 

1. Create your dataset in Labelbox from Databricks. You can specify an [IAM integration](https://docs.labelbox.com/docs/iam-delegated-access) if desired (or set to none with iam_integration=None). The below example creates a dataset with the default IAM integration set in the Labelbox account.

```
LB_dataset = labelspark.create_dataset(labelbox_client, spark_dataframe, dataset_name="Sample Dataset", 
                                      iam_integration='DEFAULT', metadata_index = dictionary_of_metadata_columns)
```
Where "spark_dataframe" is your dataframe of unstructured data with asset names and asset URLs in two columns, named "global_key" and "row_data" respectively. The metadata_index is an optional parameter if you wish to import metadata from other Spark table columns to Labelbox.

| global_key  | row_data                             | optional_metadata_field | optional_metadata_field_2| ... |
|-------------|--------------------------------------|-------------------------|--------------------------|-----|
| image1.jpg  | https://url_to_your_asset/image1.jpg |  "Example string 1"     |          1234            | ... |
| image2.jpg  | https://url_to_your_asset/image2.jpg |  "Example string 2"     |          88.8            | ... |
| image3.jpg  | https://url_to_your_asset/image3.jpg |  "Example string 3"     |          123.5           | ... |

The metadata_index dictionary follows this pattern: {column_name: metadata_type} where metadata_type is one of the following strings: "enum", "string", "number", or "datetime". In the above example, the metadata_index_dictionary would look like this: 
```
metadata_labelbox_data_types = {
  "optional_metadata_field" : "string",
  "optional_metadata_field_2" : "number"
  }
```
*Note: This library will set reserved field "lb_integration_source" in Labelbox metadata to "Databricks" automatically. This allows for enhanced search capabilities in Labelbox Catalog, and more transparent data lineage.*

Visit [this page](https://docs.labelbox.com/docs/datarow-metadata) for more information about metadata in Labelbox. 

2. Pull your raw annotations back into Databricks. 
```
bronze_DF = labelspark.get_annotations(labelbox_client,"labelbox_project_id_here", spark, sc) 
```

3. You can use the our flattener to flatten the "Label" JSON column into component columns, or use the silver table method to produce a more queryable table of your labeled assets. Both of these methods take in the bronze table of annotations from above: 

```
flattened_bronze_DF = labelspark.flatten_bronze_table(bronze_DF)
queryable_silver_DF = labelspark.bronze_to_silver(bronze_DF)
```

### How To Get Video Project Annotations

Because Labelbox Video projects can contain multiple videos, you must use the `get_videoframe_annotations` method to return an array of DataFrames for each video in your project. Each DataFrame contains frame-by-frame annotation for a video in the project: 

```
bronze_video = labelspark.get_annotations(labelbox_client,"labelbox_video_project_id_here", spark, sc) 
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
* [Visit our docs](https://docs.labelbox.com/reference/install-python-sdk) to learn how the SDK works
* Checkout our [notebook examples](https://github.com/Labelbox/labelspark/tree/master/notebooks) to follow along with interactive tutorials
* view our [API reference](https://labelbox-python.readthedocs.io/en/latest/).

## Authentication

Labelbox uses API keys to validate requests. You can create and manage API keys on [Labelbox](https://app.labelbox.com/account/api-keys). We recommend using the Databricks Secrets API to store your key. If you don't have the Secrets API, you can store your API key in a separate notebook ignored by version control. 


## Contribution
Please consult `CONTRIB.md`


