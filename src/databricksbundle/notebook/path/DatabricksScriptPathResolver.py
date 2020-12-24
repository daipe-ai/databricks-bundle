import sys
from pathlib import Path
from databricksbundle.notebook.path.NotebookPathResolverInterface import NotebookPathResolverInterface

class DatabricksScriptPathResolver(NotebookPathResolverInterface):

    def resolve(self) -> Path:
        if len(sys.argv) == 1:
            raise Exception('spark_python_task.parameters in Databricks job configuration must contain real pipeline path')

        return Path(sys.argv[1])
