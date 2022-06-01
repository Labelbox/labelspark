# this code block is needed for backwards compatibility with older Spark versions
from pyspark import SparkContext
from packaging import version
from typing import Optional, List
import labelbox
from labelbox.schema.data_row_metadata import DataRowMetadataField

sc = SparkContext.getOrCreate()
if version.parse(sc.version) < version.parse("3.2.0"):
    import databricks.koalas as pd
    needs_koalas = True
else:
    import pyspark.pandas as pd
    needs_koalas = False


def get_schema_id_for_metadata_field(client: labelbox.client.Client, metadata_field_name: str) -> str:
    # This method searches the metadata schemas in labelbox, and returns its uid if found. Otherwise, raise exception and ask users to create it in Labelbox.
    # TODO: in the future, we can automatically create it for them.
    mdo = client.get_data_row_metadata_ontology()
    try:
        schema = mdo.reserved_by_name[metadata_field_name]
        return schema.uid
    except KeyError as e:
        pass

    try:
        schema = mdo.custom_by_name[metadata_field_name]
        return schema.uid
    except KeyError as e:
        raise Exception(
            f"Metadata schema {metadata_field_name} has not been created in Labelbox UI. Please go to URL to create a custom metadata schema first.")

# upload spark dataframe to Labelbox
def create_dataset(client, spark_dataframe, iam_integration='DEFAULT', metadata_headers: List[str] = None, **kwargs):
    # expects spark dataframe to have two columns: external_id, row_data
    # external_id is the asset name ex: "photo.jpg"
    # row_data is the URL to the asset
    if needs_koalas:
        spark_dataframe = spark_dataframe.to_koalas()
    else:
        spark_dataframe = spark_dataframe.to_pandas_on_spark()
    dataset_new = client.create_dataset(
        iam_integration=iam_integration, **kwargs)
    
    if metadata_headers == None:
        data_rows = [{
            "external_id": row['external_id'],
            "row_data": row['row_data']
        } for index, row in spark_dataframe.iterrows()]
    else:
      data_rows = []
      for index, row in spark_dataframe.iterrows():
          # a list of metadata -- dict of {"schema_id":schema_id,  "value": value}
          metadata_fields = []
          for metadata_header in metadata_headers:
              metadata_fields.append(DataRowMetadataField(
                  schema_id=get_schema_id_for_metadata_field(
                      client, metadata_header),
                  value=row[metadata_header])
              )

          data_row = {
              "external_id": row['external_id'],
              "row_data": row['row_data'],
              "metadata_fields": metadata_fields,
          }
          data_rows.append(data_row)

    upload_task = dataset_new.create_data_rows(data_rows)
    upload_task.wait_till_done()
    print("Dataset created in Labelbox.")
    return dataset_new
