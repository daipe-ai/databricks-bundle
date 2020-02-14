from importlib.util import find_spec

def isDatabricks():
    return find_spec('pyspark.dbutils') is None
