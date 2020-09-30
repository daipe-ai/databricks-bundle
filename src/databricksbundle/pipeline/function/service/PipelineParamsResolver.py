from pathlib import Path
from injecta.container.ContainerInterface import ContainerInterface
from databricksbundle.pipeline.function.service.ServiceResolverInterface import ServiceResolverInterface

class PipelineParamsResolver(ServiceResolverInterface):

    def __init__(
        self,
        container: ContainerInterface,
    ):
        self.__container = container

    def resolve(self, pipelinePath: Path):
        pipelineName = f'{pipelinePath.parent.parent.stem}.{pipelinePath.parent.stem}'
        parameters = self.__container.getParameters()

        if 'datalakebundle' not in parameters:
            raise Exception('Install and activate datalake-bundle to use pipeline params')

        if pipelineName not in parameters.datalakebundle.tables:
            raise Exception(f'Pipeline {pipelineName} is not defined in datalakebundle.tables')

        if 'params' not in parameters.datalakebundle.tables[pipelineName]:
            raise Exception(f'No pipeline params defined for {pipelineName} in datalakebundle.tables')

        return parameters.datalakebundle.tables[pipelineName].params
