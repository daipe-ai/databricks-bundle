from pathlib import Path
from typing import List
from injecta.service.class_.InspectedArgument import InspectedArgument
from databricksbundle.notebook.function.ArgumentResolver import ArgumentResolver

class ArgumentsResolver:

    def __init__(
        self,
        argumentResolver: ArgumentResolver,
    ):
        self.__argumentResolver = argumentResolver

    def resolve(self, inspectedArguments: List[InspectedArgument], decoratorArgs: tuple, notebookPath: Path):
        def resolve(functionArgument: InspectedArgument, decoratorArgument):
            return self.__argumentResolver.resolve(functionArgument, decoratorArgument, notebookPath)

        decoratorArgsCount = len(decoratorArgs)

        if decoratorArgsCount > len(inspectedArguments):
            raise Exception('There are more decorator arguments than function arguments')

        return tuple(resolve(functionArgument, decoratorArgs[idx] if idx < decoratorArgsCount else None) for idx, functionArgument in enumerate(inspectedArguments))
