import re
from injecta.container.ContainerInterface import ContainerInterface
from pyfonybundles.Bundle import Bundle
from databricksbundle.notebook.NotebookErrorHandler import setNotebookErrorHandler
from databricksbundle.detector import isDatabricks
from databricksbundle.notebook.helpers import getNotebookPath, isNotebookEnvironment

class DatabricksBundle(Bundle):

    DATABRICKS_NOTEBOOK = 'databricks_notebook.yaml'
    DATABRICKS_SCRIPT = 'databricks_script.yaml'
    DATABRICKS_CONNECT = 'databricks_connect.yaml'
    DATABRICKS_TEST = 'databricks_test.yaml'

    @staticmethod
    def autodetect():
        def getDatabricksConfig():
            if isDatabricks():
                if isNotebookEnvironment():
                    return DatabricksBundle.DATABRICKS_NOTEBOOK

                return DatabricksBundle.DATABRICKS_SCRIPT

            return DatabricksBundle.DATABRICKS_CONNECT

        return DatabricksBundle(getDatabricksConfig())

    @staticmethod
    def createForTesting():
        return DatabricksBundle(DatabricksBundle.DATABRICKS_TEST)

    def __init__(self, databricksConfig: str):
        self.__databricksConfig = databricksConfig

    def getConfigFiles(self):
        return ['config.yaml', 'databricks/' + self.__databricksConfig]

    def boot(self, container: ContainerInterface):
        parameters = container.getParameters()

        if (
            isDatabricks()
            and isNotebookEnvironment()
            and parameters.databricksbundle.enableNotebookErrorHandler is True
            and not re.match('^/Users/', getNotebookPath())
        ):
            setNotebookErrorHandler()
