import sys
from pathlib import Path
from databricksbundle.detector import isDatabricks
from databricksbundle.notebook.helpers import isNotebookEnvironment

class NotebookPathResolver:

    def resolve(self) -> Path:
        if isDatabricks():
            if isNotebookEnvironment():
                from databricksbundle.notebook.helpers import getNotebookPath  # pylint: disable = import-outside-toplevel

                return Path(getNotebookPath())

            if len(sys.argv) == 1:
                raise Exception('spark_python_task.parameters in Databricks job configuration must contain real pipeline path')

            return Path(sys.argv[1])

        return Path(sys.argv[0])
