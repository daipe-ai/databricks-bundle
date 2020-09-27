# pylint: disable = invalid-name
import sys
from pathlib import Path
from typing import Tuple
from databricksbundle.detector import isDatabricks
from databricksbundle.display import display
from databricksbundle.notebook.helpers import isNotebookEnvironment
from databricksbundle.pipeline.decorator.containerLoader import containerInitEnvVarDefined
from databricksbundle.pipeline.function.ServicesResolver import ServicesResolver
from databricksbundle.pipeline.decorator.argsChecker import checkArgs
from databricksbundle.pipeline.decorator.executor.dataFrameLoader import loadDataFrame
from databricksbundle.pipeline.decorator.executor.transformation import transform
from databricksbundle.pipeline.decorator.executor.dataFrameSaver import saveDataFrame

def _getContainer():
    if containerInitEnvVarDefined():
        from databricksbundle.container.envVarContainerLoader import container # pylint: disable = import-outside-toplevel
    else:
        from databricksbundle.container.pyprojectContainerLoader import container # pylint: disable = import-outside-toplevel

    return container

def _getPipelinePath():
    if isDatabricks():
        if isNotebookEnvironment():
            from databricksbundle.notebook.helpers import getNotebookPath # pylint: disable = import-outside-toplevel

            return Path(getNotebookPath())

        if len(sys.argv) == 1:
            raise Exception('spark_python_task.parameters in Databricks job configuration must contain real pipeline path')

        return Path(sys.argv[1])

    return Path(sys.argv[0])

def _resolveServices(fun, index: int):
    return _getContainer().get(ServicesResolver).resolve(fun, index, _getPipelinePath())  # pylint: disable = no-member

def _pipelineFunctionExecuted(fun):
    return fun.__module__ == '__main__'

class pipelineFunction:

    def __init__(self, *args, **kwargs): # pylint: disable = unused-argument
        checkArgs(args, self.__class__.__name__)

    def __call__(self, fun, *args, **kwargs):
        if _pipelineFunctionExecuted(fun):
            services = _resolveServices(fun, 0)
            fun(*services)

        return fun

class dataFrameLoader:

    def __init__(self, *args, **kwargs): # pylint: disable = unused-argument
        self._displayEnabled = kwargs.get('display', False)
        checkArgs(args, self.__class__.__name__)

    def __call__(self, fun, *args, **kwargs):
        if _pipelineFunctionExecuted(fun):
            services = _resolveServices(fun, 0)
            df = loadDataFrame(fun, services)

            if self._displayEnabled:
                display(df)

        return fun

class transformation:

    def __init__(self, *args, **kwargs): # pylint: disable = unused-argument
        self._sources = args # type: Tuple[callable]
        self._displayEnabled = kwargs.get('display', False)

    def __call__(self, fun, *args, **kwargs):
        if _pipelineFunctionExecuted(fun):
            services = _resolveServices(fun, len(self._sources))
            df = transform(fun, self._sources, services)

            if self._displayEnabled:
                display(df)

        return fun

class dataFrameSaver:

    def __init__(self, *args):
        self._sources = args # type: Tuple[callable]

    def __call__(self, fun, *args, **kwargs):
        if _pipelineFunctionExecuted(fun):
            services = _resolveServices(fun, 1)
            saveDataFrame(fun, self._sources, services)

        return fun
