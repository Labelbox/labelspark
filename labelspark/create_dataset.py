#this code block is needed for backwards compatibility with older Spark versions
try:
  import pyspark.pandas as pd
  needs_koalas = False
except:
  import databricks.koalas as pd
  needs_koalas = True

# upload spark dataframe to Labelbox
def create_dataset(client, spark_dataframe, iam_integration = 'DEFAULT', **kwargs):
    # expects spark dataframe to have two columns: external_id, row_data
    # external_id is the asset name ex: "photo.jpg"
    # row_data is the URL to the asset
    if needs_koalas:
      spark_dataframe = spark_dataframe.to_koalas()
    else:
      spark_dataframe = spark_dataframe.to_pandas_on_spark()
    dataSet_new = client.create_dataset(iam_integration = iam_integration, **kwargs)

    # ported Pandas code
    data_row_urls = [{
        "external_id": row['external_id'],
        "row_data": row['row_data']
    } for index, row in spark_dataframe.iterrows()]
    upload_task = dataSet_new.create_data_rows(data_row_urls)
    upload_task.wait_till_done()
    print("Dataset created in Labelbox.")
    return dataSet_new
