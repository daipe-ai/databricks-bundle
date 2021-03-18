from pathlib import Path
from databricksbundle.notebook.helpers import get_notebook_path
from databricksbundle.notebook.path.NotebookPathResolverInterface import NotebookPathResolverInterface


class DatabricksNotebookPathResolver(NotebookPathResolverInterface):
    def resolve(self) -> Path:
        return Path(get_notebook_path())
