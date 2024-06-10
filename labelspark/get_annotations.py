import urllib
from labelspark.jsonToDataFrame import jsonToDataFrame
from labelspark.dataframe_schema_enrichment import dataframe_schema_enrichment

# returns raw bronze annotations
def get_annotations(client, project_id, spark, sc):
    project = client.get_project(project_id)
    task = project.export_v2()
    task.wait_till_done()
    data = task.result
    
    bronze_table = jsonToDataFrame(data, spark, sc)
    bronze_table = dataframe_schema_enrichment(bronze_table)
    return bronze_table