from typing import Tuple
from pyspark.sql import DataFrame

def transform(fun: callable, sources: Tuple[callable], arguments: tuple) -> DataFrame:
    g = fun.__globals__

    def transformSource(source: callable):
        return g[source.__name__ + '_df']

    dataframesToUse = tuple(map(transformSource, sources))

    df = fun(*(dataframesToUse + arguments))
    g[fun.__name__ + '_df'] = df

    return df
