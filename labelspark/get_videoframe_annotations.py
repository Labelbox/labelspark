import requests  # we need to handle a large frame-by-frame dataset for videos, so we use requests
import json

#this code block is needed for backwards compatibility with older Spark versions
from pyspark import SparkContext
from packaging import version
try:
  import pyspark.pandas as pd
except:
  import databricks.koalas as pd 
needs_koalas = False

from labelspark.jsonToDataFrame import jsonToDataFrame

def get_videoframe_annotations(bronze_video_labels, api_key, spark, sc):
    # This method takes in the bronze table from get_annotations and produces
    # an array of bronze dataframes containing frame labels for each project
    bronze_video_labels = bronze_video_labels.withColumnRenamed(
        "DataRow ID", "DataRowID")
    if needs_koalas:
        bronze = bronze_video_labels.to_koalas()
    else:
        bronze = bronze_video_labels.to_pandas_on_spark()

    # We manually build a string of frame responses to leverage our existing jsonToDataFrame code, which takes in JSON
    headers = {'Authorization': f"Bearer {api_key}"}
    master_array_of_json_arrays = []
    for index, row in bronze.iterrows():
        response = requests.get(row.Label.frames, headers=headers, stream=False)
        data = []
        for line in response.iter_lines():
            data.append({
                "DataRow ID": row.DataRowID,
                "Label": json.loads(line.decode('utf-8'))
            })
        massive_string_of_responses = json.dumps(data)
        master_array_of_json_arrays.append(massive_string_of_responses)

    array_of_bronze_dataframes = []
    for frameset in master_array_of_json_arrays:
        array_of_bronze_dataframes.append(jsonToDataFrame(frameset, spark, sc))

    return array_of_bronze_dataframes
