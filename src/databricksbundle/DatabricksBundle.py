from injecta.container.ContainerInterface import ContainerInterface
from pyfonybundles.Bundle import Bundle
from databricksbundle.notebook.NotebookErrorHandler import setNotebookErrorHandler
from databricksbundle.detector import isDatabricks

class DatabricksBundle(Bundle):

    def boot(self, container: ContainerInterface):
        if isDatabricks() and container.getConfig().databricksbundle.enableNotebookErrorHandler is True:
            setNotebookErrorHandler()
