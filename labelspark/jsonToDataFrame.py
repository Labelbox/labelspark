def jsonToDataFrame(json, spark, sc, schema=None):
    # code taken from Databricks tutorial https://docs.azuredatabricks.net/_static/notebooks/transform-complex-data-types-python.html
    reader = spark.read
    if schema:
        reader.schema(schema)
    return reader.json(sc.parallelize([json]))