# pylint: disable = invalid-name
import os
import sys
from pathlib import Path
from typing import Tuple
from databricksbundle.pipeline.function.ServicesResolver import ServicesResolver
from databricksbundle.pipeline.Pipeline import Pipeline
from pyfonybundles.appContainerInit import initAppContainer
from databricksbundle.pipeline.decorator.static_init import static_init
from databricksbundle.pipeline.decorator.argsChecker import checkArgs
from databricksbundle.pipeline.pipelineInvoker import invokePipeline

@static_init
class PipelineDecorator:

    _servicesResolver: ServicesResolver
    pipeline: Pipeline

    @classmethod
    def static_init(cls):
        container = initAppContainer(os.environ['APP_ENV'])

        cls._servicesResolver = container.get(ServicesResolver)
        cls.pipeline = Pipeline()

class dataFrameLoader(PipelineDecorator):

    def __init__(self, *args, **kwargs): # pylint: disable = unused-argument
        checkArgs(args)

    def __call__(self, fun, *args, **kwargs):
        pipelinePath = Path(sys.argv[0])
        services = self._servicesResolver.resolve(fun, 0, pipelinePath) # pylint: disable = no-member

        PipelineDecorator.pipeline.addDataFrameLoader(fun, services)
        return fun

class transformation(PipelineDecorator):

    def __init__(self, *args: Tuple[callable], **kwargs): # pylint: disable = unused-argument
        self._sources = args

    def __call__(self, fun, *args, **kwargs):
        startIndex = len(self._sources)
        pipelinePath = Path(sys.argv[0])
        services = self._servicesResolver.resolve(fun, startIndex, pipelinePath) # pylint: disable = no-member
        PipelineDecorator.pipeline.addTransformation(fun, self._sources, services)
        return fun

class dataFrameSaver(PipelineDecorator):

    def __init__(self, *args: Tuple[callable]):
        self._sources = args

    def __call__(self, fun, *args, **kwargs):
        dataFramesByName = invokePipeline(PipelineDecorator.pipeline)

        def transformSource(source: callable):
            return dataFramesByName[source.__name__ + '_df']

        pipelinePath = Path(sys.argv[0])
        dataframesToUse = tuple(map(transformSource, self._sources))
        services = self._servicesResolver.resolve(fun, 1, pipelinePath) # pylint: disable = no-member

        fun(*(dataframesToUse + services))

        return fun
