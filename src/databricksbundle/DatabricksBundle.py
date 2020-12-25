import re
from typing import List
from box import Box
from databricksbundle.bootstrap import bootstrapConfigReader
from consolebundle.detector import isRunningInConsole
from injecta.container.ContainerInterface import ContainerInterface
from injecta.dtype.DType import DType
from injecta.package.pathResolver import resolvePath
from injecta.service.Service import Service
from injecta.service.ServiceAlias import ServiceAlias
from injecta.service.argument.ServiceArgument import ServiceArgument
from pyfonybundles.Bundle import Bundle
from databricksbundle.notebook.NotebookErrorHandler import setNotebookErrorHandler
from databricksbundle.detector import isDatabricks
from databricksbundle.notebook.helpers import getNotebookPath, isNotebookEnvironment
from databricksbundle.notebook.logger.NotebookLoggerFactory import NotebookLoggerFactory

class DatabricksBundle(Bundle):

    DATABRICKS_NOTEBOOK = 'databricks_notebook.yaml'
    DATABRICKS_SCRIPT = 'databricks_script.yaml'
    DATABRICKS_CONNECT = 'databricks_connect.yaml'

    @staticmethod
    def autodetect():
        if isDatabricks():
            if isNotebookEnvironment():
                return DatabricksBundle(DatabricksBundle.DATABRICKS_NOTEBOOK)

            return DatabricksBundle(DatabricksBundle.DATABRICKS_SCRIPT)

        return DatabricksBundle(DatabricksBundle.DATABRICKS_CONNECT)

    def __init__(self, databricksConfig: str):
        self.__databricksConfig = databricksConfig

    def getConfigFiles(self):
        return ['config.yaml', 'databricks/' + self.__databricksConfig]

    def modifyRawConfig(self, rawConfig: dict) -> dict:
        bootstrapConfig = bootstrapConfigReader.read()

        if 'bricksflow' in rawConfig['parameters']:
            raise Exception('parameters.bricksflow must not be explicitly defined')

        rawConfig['parameters']['bricksflow'] = {
            'rootModule': {
                'name': bootstrapConfig.rootModuleName,
                'path': resolvePath(bootstrapConfig.rootModuleName).replace('\\', '/'),
            }
        }

        return rawConfig

    def modifyServices(self, services: List[Service], aliases: List[ServiceAlias], parameters: Box): # pylint: disable = unused-argument
        if isRunningInConsole():
            aliases.append(ServiceAlias('databricksbundle.logger', 'consolebundle.logger'))
        else:
            service = Service('databricksbundle.logger', DType('logging', 'Logger'))
            service.setFactory(ServiceArgument(NotebookLoggerFactory.__module__), 'create')

            services.append(service)

        return services, aliases

    def boot(self, container: ContainerInterface):
        parameters = container.getParameters()

        if (
            isDatabricks()
            and isNotebookEnvironment()
            and parameters.databricksbundle.enableNotebookErrorHandler is True
            and not re.match('^/Users/', getNotebookPath())
        ):
            setNotebookErrorHandler()
