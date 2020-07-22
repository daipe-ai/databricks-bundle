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

        cls._servicesResolver = container.get(ServicesResolver)

class dataFrameLoader(PipelineDecorator):

    def __init__(self, *args, **kwargs):
        self._displayEnabled = kwargs.get('display', False)
        checkArgs(args)

    def __call__(self, fun, *args, **kwargs):
        pipelinePath = Path(getNotebookPath())
        services = self._servicesResolver.resolve(fun, 0, pipelinePath) # pylint: disable = no-member

        g = fun.__globals__
        df = fun(*services)
        g[fun.__name__ + '_df'] = df

        if self._displayEnabled:
            display(df)

        return fun

class transformation(PipelineDecorator):

    def __init__(self, *args: Tuple[callable], **kwargs):
        self._sources = args
        self._displayEnabled = kwargs.get('display', False)

    def __call__(self, fun, *args, **kwargs):
        g = fun.__globals__

        def transformSource(source: callable):
            return g[source.__name__ + '_df']

        pipelinePath = Path(getNotebookPath())
        dataframesToUse = tuple(map(transformSource, self._sources))
        startIndex = len(self._sources)
        services = self._servicesResolver.resolve(fun, startIndex, pipelinePath) # pylint: disable = no-member

        df = fun(*(dataframesToUse + services))
        g[fun.__name__ + '_df'] = df

        if self._displayEnabled:
            display(df)

        return fun

class dataFrameSaver(PipelineDecorator):

    def __init__(self, *args: Tuple[callable]):
        self._sources = args

    def __call__(self, fun, *args, **kwargs):
        g = fun.__globals__

        def transformSource(source: callable):
            return g[source.__name__ + '_df']

        pipelinePath = Path(getNotebookPath())
        dataframesToUse = tuple(map(transformSource, self._sources))
        services = self._servicesResolver.resolve(fun, 1, pipelinePath) # pylint: disable = no-member

        fun(*(dataframesToUse + services))

        return fun
