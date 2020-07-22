from typing import Dict, Tuple

class Pipeline:

    _dataFrameLoaders: Dict[str, callable] = dict()
    _transformations: Dict[str, callable] = dict()
    _sources: Dict[str, Tuple[callable]] = dict()
    _services: Dict[str, Tuple[object]] = dict()

    def addDataFrameLoader(self, fun: callable, services: Tuple[object]):
        functionName = fun.__name__

        if functionName not in self._dataFrameLoaders:
            self._dataFrameLoaders[functionName] = fun
            self._services[functionName] = services

    def addTransformation(self, fun: callable, sources: Tuple[callable], services: Tuple[object]):
        functionName = fun.__name__

        if functionName not in self._transformations:
            self._transformations[functionName] = fun
            self._sources[functionName] = sources
            self._services[functionName] = services

    def getSource(self, functionName) -> Tuple[callable]:
        return self._sources[functionName]

    def getServices(self, functionName) -> Tuple[object]:
        return self._services[functionName]

    def getTransformations(self) -> Dict[str, callable]:
        return self._transformations

    def getDataFrameLoaders(self) -> Dict[str, callable]:
        return self._dataFrameLoaders
