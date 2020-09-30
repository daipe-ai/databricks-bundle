from pathlib import Path
from loggerbundle.LoggerFactory import LoggerFactory
from databricksbundle.pipeline.function.service.ServiceResolverInterface import ServiceResolverInterface

class LoggerResolver(ServiceResolverInterface):

    def __init__(
        self,
        loggerFactory: LoggerFactory,
    ):
        self.__loggerFactory = loggerFactory

    def resolve(self, pipelinePath: Path):
        pipelineName = f'{pipelinePath.parent.parent.stem}.{pipelinePath.parent.stem}'

        return self.__loggerFactory.create(pipelineName)
