def check_for_pyspark():
    try:
        import pyspark.pandas as pd
    except:
        raise RuntimeError(f'labelspark.Client() requires pyspark to be installed - please update your Databricks runtime to support pyspark')
