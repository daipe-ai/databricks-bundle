from typing import List
from injecta.service.class_.InspectedArgument import InspectedArgument
from databricksbundle.notebook.function.ArgumentResolver import ArgumentResolver

class ArgumentsResolver:

    def __init__(
        self,
        argumentResolver: ArgumentResolver,
    ):
        self.__argumentResolver = argumentResolver

    def resolve(self, inspectedArguments: List[InspectedArgument], decoratorArgs: tuple):
        decoratorArgsCount = len(decoratorArgs)

        if decoratorArgsCount > len(inspectedArguments):
            raise Exception('There are more decorator arguments than function arguments')

        return tuple(self.__argumentResolver.resolve(functionArgument, decoratorArgs[idx] if idx < decoratorArgsCount else None)
                for idx, functionArgument in enumerate(inspectedArguments))
