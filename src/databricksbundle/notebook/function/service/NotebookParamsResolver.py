from pathlib import Path
from injecta.container.ContainerInterface import ContainerInterface
from databricksbundle.notebook.function.service.ServiceResolverInterface import ServiceResolverInterface

class NotebookParamsResolver(ServiceResolverInterface):

    def __init__(
        self,
        container: ContainerInterface,
    ):
        self.__container = container

    def resolve(self, notebookPath: Path):
        notebookName = f'{notebookPath.parent.parent.stem}.{notebookPath.parent.stem}'
        parameters = self.__container.getParameters()

        if 'datalakebundle' not in parameters:
            raise Exception('Install and activate datalake-bundle to use notebook-specific params')

        if notebookName not in parameters.datalakebundle.tables:
            raise Exception(f'Notebook {notebookName} is not defined in datalakebundle.tables')

        if 'params' not in parameters.datalakebundle.tables[notebookName]:
            raise Exception(f'No notebook params defined for {notebookName} in datalakebundle.tables')

        return parameters.datalakebundle.tables[notebookName].params
