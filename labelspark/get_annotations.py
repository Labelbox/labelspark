import urllib
from labelspark.jsonToDataFrame import jsonToDataFrame
from labelspark.dataframe_schema_enrichment import dataframe_schema_enrichment

# returns raw bronze annotations
def get_annotations(client, project_id, spark, sc):
    project = client.get_project(project_id)
    with urllib.request.urlopen(project.export_labels()) as url:
        api_response_string = url.read().decode()  # this is a string of JSONs
    bronze_table = jsonToDataFrame(api_response_string, spark, sc)
    bronze_table = dataframe_schema_enrichment(bronze_table)
    return bronze_table