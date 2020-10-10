from pathlib import Path
from loggerbundle.LoggerFactory import LoggerFactory
from databricksbundle.notebook.function.service.ServiceResolverInterface import ServiceResolverInterface

class LoggerResolver(ServiceResolverInterface):

    def __init__(
        self,
        loggerFactory: LoggerFactory,
    ):
        self.__loggerFactory = loggerFactory

    def resolve(self, notebookPath: Path):
        notebookName = f'{notebookPath.parent.parent.stem}.{notebookPath.parent.stem}'

        return self.__loggerFactory.create(notebookName)
