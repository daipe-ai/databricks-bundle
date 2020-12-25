from logging import Logger
from loggerbundle.LoggerFactory import LoggerFactory
from databricksbundle.notebook.path.NotebookPathResolverInterface import NotebookPathResolverInterface

class NotebookLoggerFactory:

    def __init__(
        self,
        notebookPathResolver: NotebookPathResolverInterface,
        loggerFactory: LoggerFactory,
    ):
        self.__notebookPathResolver = notebookPathResolver
        self.__loggerFactory = loggerFactory

    def create(self) -> Logger:
        notebookPath = self.__notebookPathResolver.resolve()

        return self.__loggerFactory.create(notebookPath.parent.stem)
