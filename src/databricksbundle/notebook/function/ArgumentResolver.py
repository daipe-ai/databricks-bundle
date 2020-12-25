from logging import Logger
from injecta.container.ContainerInterface import ContainerInterface
from injecta.dtype.AbstractType import AbstractType
from injecta.dtype.classLoader import loadClass
from injecta.parameter.allPlaceholdersReplacer import replaceAllPlaceholders, findAllPlaceholders
from injecta.service.class_.InspectedArgument import InspectedArgument
from databricksbundle.notebook.decorator.StringableParameterInterface import StringableParameterInterface

class ArgumentResolver:

    def __init__(
        self,
        logger: Logger,
        container: ContainerInterface,
    ):
        self.__logger = logger
        self.__container = container

    def resolve(self, functionArgument: InspectedArgument, decoratorArgument):
        if decoratorArgument is not None:
            return self.__resolveExplicitValue(functionArgument, decoratorArgument)

        argumentType = functionArgument.dtype

        if functionArgument.hasDefaultValue():
            return self.__checkType(functionArgument.defaultValue, argumentType, functionArgument.name)

        if not argumentType.isDefined():
            raise Exception(f'Argument "{functionArgument.name}" must either have explicit value, default value or typehint defined')

        if str(argumentType) == 'logging.Logger':
            return self.__logger

        class_ = loadClass(argumentType.moduleName, argumentType.className)  # pylint: disable = invalid-name

        return self.__container.get(class_)

    def __resolveExplicitValue(self, functionArgument: InspectedArgument, decoratorArgument):
        argumentType = functionArgument.dtype

        if isinstance(decoratorArgument, str):
            output = self.__resolveStringArgument(decoratorArgument)
            return self.__checkType(output, argumentType, functionArgument.name)
        if isinstance(decoratorArgument, StringableParameterInterface):
            output = self.__resolveStringArgument(decoratorArgument.toString())
            return self.__checkType(output, argumentType, functionArgument.name)
        # isinstance(decoratorArgument, AbstractDecorator) does not work probably due to some cyclic import
        if hasattr(decoratorArgument, '_isDecorator') and decoratorArgument._isDecorator is True: # pylint: disable = protected-access
            return decoratorArgument.result

        return self.__checkType(decoratorArgument, argumentType, functionArgument.name)

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
