from typing import List, Tuple

def saveDataFrame(fun: callable, sources: Tuple[callable], services: List[object]):
    g = fun.__globals__

    def transformSource(source: callable):
        return g[source.__name__ + '_df']

    dataframesToUse = tuple(map(transformSource, sources))

    fun(*(dataframesToUse + services))
