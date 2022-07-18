#this code block is needed for backwards compatibility with older Spark versions
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

# upload spark dataframe to Labelbox
def create_dataset(client, spark_dataframe, iam_integration = 'DEFAULT', **kwargs):
    # expects spark dataframe to have two columns: external_id, row_data
    # external_id is the asset name ex: "photo.jpg"
    # row_data is the URL to the asset
    if needs_koalas:
      spark_dataframe = spark_dataframe.to_koalas()
    else:
      spark_dataframe = spark_dataframe.to_pandas_on_spark()
    dataset_new = client.create_dataset(iam_integration = iam_integration, **kwargs)

    
    metadata_ontology = client.get_data_row_metadata_ontology()
    ## Add logic to create a new metadata schema called "data_source" if not already existed
    all_schema_names = [field.name for field in metadata_ontology.fields]
    if "lb_partner_source" not in all_schema_names:
      # create new metadata schema
      metadata_schema = metadata_ontology.create_schema(name="lb_partner_source", kind=DataRowMetadataKind.string)
      schema_id = metadata_schema.uid
    else:
      schema_id = metadata_ontology.custom_by_name["lb_partner_source"].uid

    partner_source_metadata = DataRowMetadataField(
        schema_id=schema_id,  # specify the schema id
        value="Databricks", 
    )


    # ported Pandas code
    data_row_urls = [{
        "external_id": row['external_id'],
        "row_data": row['row_data'],
        "metadata_fields": [partner_source_metadata], 
    } for index, row in spark_dataframe.iterrows()]
    upload_task = dataset_new.create_data_rows(data_row_urls)
    upload_task.wait_till_done()
    print("Dataset created in Labelbox.")
    return dataset_new