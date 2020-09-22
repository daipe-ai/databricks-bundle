# pylint: disable = invalid-name
from pathlib import Path
from typing import Tuple
from databricksbundle.display import display
from databricksbundle.pipeline.function.ServicesResolver import ServicesResolver
from databricksbundle.pipeline.decorator.argsChecker import checkArgs
from databricksbundle.notebook.helpers import getNotebookPath
from databricksbundle.pipeline.decorator.executor.dataFrameLoader import loadDataFrame
from databricksbundle.pipeline.decorator.executor.transformation import transform
from databricksbundle.pipeline.decorator.executor.dataFrameSaver import saveDataFrame

def _resolveServices(fun, index: int):
    from databricksbundle.container.envVarContainerLoader import container # pylint: disable = import-outside-toplevel

    return container.get(ServicesResolver).resolve(fun, index, Path(getNotebookPath()))  # pylint: disable = no-member

class pipelineFunction:

    def __init__(self, *args, **kwargs): # pylint: disable = unused-argument
        checkArgs(args, self.__class__.__name__)

    def __call__(self, fun, *args, **kwargs):
        services = _resolveServices(fun, 0)
        fun(*services)

        return fun

class dataFrameLoader:

    def __init__(self, *args, **kwargs):
        self._displayEnabled = kwargs.get('display', False)
        checkArgs(args, self.__class__.__name__)

    def __call__(self, fun, *args, **kwargs):
        services = _resolveServices(fun, 0)
        df = loadDataFrame(fun, services)

        if self._displayEnabled:
            display(df)

        return fun

class transformation:

    def __init__(self, *args, **kwargs):
        self._sources = args # type: Tuple[callable]
        self._displayEnabled = kwargs.get('display', False)

    def __call__(self, fun, *args, **kwargs):
        services = _resolveServices(fun, len(self._sources))
        df = transform(fun, self._sources, services)

        if self._displayEnabled:
            display(df)

        return fun

class dataFrameSaver:

    def __init__(self, *args):
        self._sources = args # type: Tuple[callable]

    def __call__(self, fun, *args, **kwargs):
        services = _resolveServices(fun, 1)
        saveDataFrame(fun, self._sources, services)

        return fun
