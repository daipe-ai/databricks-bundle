from typing import List
from pyspark.sql import DataFrame

def loadDataFrame(fun: callable, services: List[object]) -> DataFrame:
    g = fun.__globals__
    df = fun(*services)
    g[fun.__name__ + '_df'] = df

    return df
