from pyspark.sql import DataFrame

def loadDataFrame(fun: callable, arguments: tuple) -> DataFrame:
    g = fun.__globals__
    df = fun(*arguments)
    g[fun.__name__ + '_df'] = df

    return df
