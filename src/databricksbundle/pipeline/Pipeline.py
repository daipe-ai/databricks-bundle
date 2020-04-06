from typing import Dict, Tuple

class Pipeline:

    _dataFrameLoaders = dict()
    _transformations = dict()
    _sources = dict()

    def addDataFrameLoader(self, fun: callable):
        functionName = fun.__name__

        if functionName not in self._dataFrameLoaders:
            self._dataFrameLoaders[functionName] = fun

    def addTransformation(self, fun: callable, sources: Tuple[callable]):
        functionName = fun.__name__

        if functionName not in self._transformations:
            self._transformations[functionName] = fun
            self._sources[functionName] = sources

    def getSource(self, name) -> Tuple[callable]:
        return self._sources[name]

    def getTransformations(self) -> Dict[str, callable]:
        return self._transformations

    def getDataFrameLoaders(self) -> Dict[str, callable]:
        return self._dataFrameLoaders
