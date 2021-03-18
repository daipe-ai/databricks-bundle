from typing import List
from injecta.service.class_.InspectedArgument import InspectedArgument
from databricksbundle.notebook.function.ArgumentResolver import ArgumentResolver


class ArgumentsResolver:
    def __init__(
        self,
        argument_resolver: ArgumentResolver,
    ):
        self.__argument_resolver = argument_resolver

    def resolve(self, inspected_arguments: List[InspectedArgument], decorator_args: tuple):
        decorator_args_count = len(decorator_args)

        if decorator_args_count > len(inspected_arguments):
            raise Exception("There are more decorator arguments than function arguments")

        return tuple(
            self.__argument_resolver.resolve(function_argument, decorator_args[idx] if idx < decorator_args_count else None)
            for idx, function_argument in enumerate(inspected_arguments)
        )
