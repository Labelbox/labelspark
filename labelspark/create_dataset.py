# this code block is needed for backwards compatibility with older Spark versions
from pyspark import SparkContext
from packaging import version
from labelbox.schema.data_row_metadata import (
    DataRowMetadataField,
    DataRowMetadataKind
)

sc = SparkContext.getOrCreate()
if version.parse(sc.version) < version.parse("3.2.0"):
    import databricks.koalas as pd
    needs_koalas = True
else:
    import pyspark.pandas as pd
    needs_koalas = False


def create_data_rows(client, dataset, spark_dataframe):
    if needs_koalas:
        spark_dataframe = spark_dataframe.to_koalas()
    else:
        spark_dataframe = spark_dataframe.to_pandas_on_spark()

    metadata_ontology = client.get_data_row_metadata_ontology()
    # Add logic to create a new metadata schema called "data_source" if not already existed
    all_schema_names = [field.name for field in metadata_ontology.fields]
    if "lb_integration_source" not in all_schema_names:
        # create new metadata schema
        metadata_schema = metadata_ontology.create_schema(
            name="lb_integration_source", kind=DataRowMetadataKind.string)
        schema_id = metadata_schema.uid
    else:
        schema_id = metadata_ontology.custom_by_name["lb_integration_source"].uid

    integration_source_metadata = DataRowMetadataField(
        schema_id=schema_id, 
        value="Databricks",
    )

    # ported Pandas code
    data_row_urls = [{
        "external_id": row['external_id'],
        "row_data": row['row_data'],
        "metadata_fields": [integration_source_metadata],
    } for index, row in spark_dataframe.iterrows()]
    upload_task = dataset.create_data_rows(data_row_urls)
    upload_task.wait_till_done()
    return dataset 


def append_to_dataset(client, spark_dataframe, dataset_id):
    """
    Append spark dataframe to Labelbox
    # Need to specify the dataset id on Labelbox. 
    """
    ## QUESTION: how about iam integration for this existing dataset? Should we just specify that users need to create dataset
    ## with the right iam integration when creating it for the first time
    dataset = client.get_dataset(dataset_id)
    dataset = create_data_rows(client, dataset, spark_dataframe)
    print(f"Appended to dataset {dataset.name} in Labelbox.")
    return dataset


def create_dataset(client, spark_dataframe, iam_integration='DEFAULT', dataset_name="Databricks Import Dataset", **kwargs):
    """
    Upload spark dataframe to Labelbox
    # expects spark dataframe to have two columns: external_id, row_data
    # external_id is the asset name ex: "photo.jpg"
    # row_data is the URL to the asset
    # dataset_name is the dataset name to be created
    """
    dataset_new = client.create_dataset(
        iam_integration=iam_integration, name=dataset_name, **kwargs)
    dataset_new = create_data_rows(client, dataset_new, spark_dataframe)
    print("Dataset created in Labelbox.")
    return dataset_new
