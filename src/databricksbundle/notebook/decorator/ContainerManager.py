import os
from databricksbundle.bootstrap import bootstrapConfigReader
from injecta.container.ContainerInterface import ContainerInterface

class ContainerManager:

    _container: ContainerInterface

    @classmethod
    def setContainer(cls, container: ContainerInterface):
        cls._container = container

    @classmethod
    def getContainer(cls):
        if not hasattr(cls, '_container'):
            cls._container = cls._createContainer()

        return cls._container

    @staticmethod
    def _createContainer():
        bootstrapConfig = bootstrapConfigReader.read()
        return bootstrapConfig.containerInitFunction(os.environ['APP_ENV'], bootstrapConfig)
