from typing import Tuple

def saveDataFrame(fun: callable, sourceFunctions: Tuple[callable], arguments: tuple):
    g = fun.__globals__

    def transformSource(source: callable):
        return g[source.__name__ + '_df']

    dataframesToUse = tuple(map(transformSource, sourceFunctions))

    fun(*(dataframesToUse + arguments))
