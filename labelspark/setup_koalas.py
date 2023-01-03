from pyspark import SparkContext
from packaging import version

def setup_koalas():
    sc = SparkContext.getOrCreate()
    return True if version.parse(sc.version) < version.parse("3.2.0") else False  
