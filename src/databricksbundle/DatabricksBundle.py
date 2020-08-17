import re
from injecta.container.ContainerInterface import ContainerInterface
from pyfonybundles.Bundle import Bundle
from databricksbundle.notebook.NotebookErrorHandler import setNotebookErrorHandler
from databricksbundle.detector import isDatabricks
from databricksbundle.notebook.helpers import getNotebookPath

class DatabricksBundle(Bundle):

    @staticmethod
    def autodetect():
        if isDatabricks():
            return DatabricksBundle('databricks.yaml')

        return DatabricksBundle('databricks_connect.yaml')

    def __init__(self, sparkConfigFilename: str):
        self.__sparkConfigFilename = sparkConfigFilename

    def getConfigFiles(self):
        return ['config.yaml', self.__sparkConfigFilename]

    def boot(self, container: ContainerInterface):
        parameters = container.getParameters()

        if (
            isDatabricks()
            and parameters.databricksbundle.enableNotebookErrorHandler is True
            and not re.match('^/Users/', getNotebookPath())
        ):
            setNotebookErrorHandler()
