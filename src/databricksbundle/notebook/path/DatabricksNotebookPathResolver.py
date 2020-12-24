from pathlib import Path
from databricksbundle.notebook.helpers import getNotebookPath
from databricksbundle.notebook.path.NotebookPathResolverInterface import NotebookPathResolverInterface

class DatabricksNotebookPathResolver(NotebookPathResolverInterface):

    def resolve(self) -> Path:
        return Path(getNotebookPath())
