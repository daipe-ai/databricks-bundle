# pylint: disable = invalid-name
import os
from pathlib import Path
from typing import Tuple
from databricksbundle.display import display
from databricksbundle.pipeline.function.ServicesResolver import ServicesResolver
from databricksbundle.pipeline.decorator.static_init import static_init
from databricksbundle.pipeline.decorator.argsChecker import checkArgs
from injecta.dtype.classLoader import loadClass
from databricksbundle.notebook.helpers import getNotebookPath
from databricksbundle.pipeline.decorator.executor.dataFrameLoader import loadDataFrame
from databricksbundle.pipeline.decorator.executor.transformation import transform
from databricksbundle.pipeline.decorator.executor.dataFrameSaver import saveDataFrame

@static_init
class PipelineDecorator:

    _servicesResolver: ServicesResolver

    @classmethod
    def static_init(cls):
        if 'CONTAINER_INIT_FUNCTION' not in os.environ:
            raise Exception('CONTAINER_INIT_FUNCTION environment variable must be set on cluster')

        containerInitFunctionPath = os.environ['CONTAINER_INIT_FUNCTION']
        containerInitModuleName = containerInitFunctionPath[0:containerInitFunctionPath.rfind('.')]
        containerInitFunctionName = containerInitFunctionPath[containerInitFunctionPath.rfind('.') + 1:]

        containerInitFunction = loadClass(containerInitModuleName, containerInitFunctionName)
        container = containerInitFunction(os.environ['APP_ENV'])

        cls._pipelinePath = Path(getNotebookPath())
        cls._servicesResolver = container.get(ServicesResolver)

class pipelineFunction(PipelineDecorator):

    def __init__(self, *args, **kwargs): # pylint: disable = unused-argument
        checkArgs(args, self.__class__.__name__)

    def __call__(self, fun, *args, **kwargs):
        services = self._servicesResolver.resolve(fun, 0, self._pipelinePath) # pylint: disable = no-member
        fun(*services)

        return fun

class dataFrameLoader(PipelineDecorator):

    def __init__(self, *args, **kwargs):
        self._displayEnabled = kwargs.get('display', False)
        checkArgs(args, self.__class__.__name__)

    def __call__(self, fun, *args, **kwargs):
        services = self._servicesResolver.resolve(fun, 0, self._pipelinePath) # pylint: disable = no-member
        df = loadDataFrame(fun, services)

        if self._displayEnabled:
            display(df)

        return fun

class transformation(PipelineDecorator):

    def __init__(self, *args, **kwargs):
        self._sources = args # type: Tuple[callable]
        self._displayEnabled = kwargs.get('display', False)

    def __call__(self, fun, *args, **kwargs):
        startIndex = len(self._sources)
        services = self._servicesResolver.resolve(fun, startIndex, self._pipelinePath) # pylint: disable = no-member
        df = transform(fun, self._sources, services)

        if self._displayEnabled:
            display(df)

        return fun

class dataFrameSaver(PipelineDecorator):

    def __init__(self, *args):
        self._sources = args # type: Tuple[callable]

    def __call__(self, fun, *args, **kwargs):
        services = self._servicesResolver.resolve(fun, 1, self._pipelinePath) # pylint: disable = no-member
        saveDataFrame(fun, self._sources, services)

        return fun
