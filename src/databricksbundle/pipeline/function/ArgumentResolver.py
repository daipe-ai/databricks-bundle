from pathlib import Path
from typing import Dict
from injecta.container.ContainerInterface import ContainerInterface
from injecta.dtype.AbstractType import AbstractType
from injecta.dtype.classLoader import loadClass
from injecta.parameter.allPlaceholdersReplacer import replaceAllPlaceholders, findAllPlaceholders
from injecta.service.class_.InspectedArgument import InspectedArgument
from databricksbundle.pipeline.function.service.ServiceResolverInterface import ServiceResolverInterface

class ArgumentResolver:

    def __init__(
        self,
        serviceResolvers: Dict[str, str],
        container: ContainerInterface
    ):
        self.__serviceResolversMapping = serviceResolvers or []
        self.__container = container

    def resolve(self, functionArgument: InspectedArgument, decoratorArgument, pipelinePath: Path):
        argumentType = functionArgument.dtype

        if decoratorArgument is not None:
            if isinstance(decoratorArgument, str):
                output = self.__resolveStringArgument(decoratorArgument)
                return self.__checkType(output, argumentType, functionArgument.name)

            return self.__checkType(decoratorArgument, argumentType, functionArgument.name)

        if functionArgument.hasDefaultValue():
            return self.__checkType(functionArgument.defaultValue, argumentType, functionArgument.name)

        if not argumentType.isDefined():
            raise Exception(f'Argument "{functionArgument.name}" must either have explicit value, default value or typehint defined')

        argumentTypeStr = str(argumentType)

        if argumentTypeStr in self.__serviceResolversMapping:
            if self.__serviceResolversMapping[argumentTypeStr][0:1] != '@':
                raise Exception(f'Service name must start with @ for argument {functionArgument.name}')

            serviceResolverName = self.__serviceResolversMapping[argumentTypeStr][1:]
            serviceResolver: ServiceResolverInterface = self.__container.get(serviceResolverName)

            return serviceResolver.resolve(pipelinePath)

        class_ = loadClass(argumentType.moduleName, argumentType.className) # pylint: disable = invalid-name

        return self.__container.get(class_)

    def __resolveStringArgument(self, decoratorArgument):
        if decoratorArgument[0:1] == '@':
            return self.__container.get(decoratorArgument[1:])

        matches = findAllPlaceholders(decoratorArgument)

        if not matches:
            return decoratorArgument

        parameters = self.__container.getParameters()

        return replaceAllPlaceholders(decoratorArgument, matches, parameters, decoratorArgument)

    def __checkType(self, value, expectedType: AbstractType, argumentName: str):
        valueTypeStr = value.__class__.__module__ + '.' + value.__class__.__name__
        expectedTypeStr = str(expectedType)

        if expectedType.isDefined() and valueTypeStr != expectedTypeStr:
            expectedTypeStr = expectedTypeStr.replace('builtins.', '')
            valueTypeStr = valueTypeStr.replace('builtins.', '')
            raise Exception(f'Argument "{argumentName}" is defined as {expectedTypeStr}, {valueTypeStr} given instead')

        return value
