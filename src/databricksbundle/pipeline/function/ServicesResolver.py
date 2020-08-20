from pathlib import Path
from databricksbundle.pipeline.function.ServiceResolver import ServiceResolver
from databricksbundle.pipeline.function.functionInspector import inspectFunction

class ServicesResolver:

    def __init__(
        self,
        serviceResolver: ServiceResolver,
    ):
        self.__serviceResolver = serviceResolver

    def resolve(self, fun: callable, startIndex: int, pipelinePath: Path):
        functionArguments = inspectFunction(fun)

        def resolve(inspectedArgument):
            return self.__serviceResolver.resolve(inspectedArgument.dtype, pipelinePath)

        return tuple(map(resolve, functionArguments[startIndex:]))
