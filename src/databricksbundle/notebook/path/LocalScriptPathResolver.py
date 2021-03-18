import sys
from pathlib import Path
from databricksbundle.notebook.path.NotebookPathResolverInterface import NotebookPathResolverInterface


class LocalScriptPathResolver(NotebookPathResolverInterface):
    def resolve(self) -> Path:
        return Path(sys.argv[0])
