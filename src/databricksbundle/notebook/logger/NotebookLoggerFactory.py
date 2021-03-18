from logging import Logger
from loggerbundle.LoggerFactory import LoggerFactory
from databricksbundle.notebook.path.NotebookPathResolverInterface import NotebookPathResolverInterface


class NotebookLoggerFactory:
    def __init__(
        self,
        notebook_path_resolver: NotebookPathResolverInterface,
        logger_factory: LoggerFactory,
    ):
        self.__notebook_path_resolver = notebook_path_resolver
        self.__logger_factory = logger_factory

    def create(self) -> Logger:
        notebook_path = self.__notebook_path_resolver.resolve()

        return self.__logger_factory.create(notebook_path.parent.stem)
