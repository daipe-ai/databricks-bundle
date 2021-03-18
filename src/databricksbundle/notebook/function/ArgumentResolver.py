from logging import Logger
from injecta.container.ContainerInterface import ContainerInterface
from injecta.dtype.AbstractType import AbstractType
from injecta.module import attribute_loader
from injecta.parameter.all_placeholders_replacer import replace_all_placeholders, find_all_placeholders
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

    def resolve(self, function_argument: InspectedArgument, decorator_argument):
        if decorator_argument is not None:
            return self.__resolve_explicit_value(function_argument, decorator_argument)

        argument_type = function_argument.dtype

        if function_argument.has_default_value():
            return self.__check_type(function_argument.default_value, argument_type, function_argument.name)

        if not argument_type.is_defined():
            raise Exception(f'Argument "{function_argument.name}" must either have explicit value, default value or typehint defined')

        if str(argument_type) == "logging.Logger":
            return self.__logger

        class_ = attribute_loader.load(argument_type.module_name, argument_type.class_name)

        return self.__container.get(class_)

    def __resolve_explicit_value(self, function_argument: InspectedArgument, decorator_argument):
        argument_type = function_argument.dtype

        if isinstance(decorator_argument, str):
            output = self.__resolve_string_argument(decorator_argument)
            return self.__check_type(output, argument_type, function_argument.name)
        if isinstance(decorator_argument, StringableParameterInterface):
            output = self.__resolve_string_argument(decorator_argument.to_string())
            return self.__check_type(output, argument_type, function_argument.name)
        # isinstance(decorator_argument, AbstractDecorator) does not work probably due to some cyclic import
        if hasattr(decorator_argument, "_is_decorator") and decorator_argument._is_decorator is True:
            return decorator_argument.result

        return self.__check_type(decorator_argument, argument_type, function_argument.name)

    def __resolve_string_argument(self, decorator_argument):
        if decorator_argument[0:1] == "@":
            return self.__container.get(decorator_argument[1:])

        matches = find_all_placeholders(decorator_argument)

        if not matches:
            return decorator_argument

        parameters = self.__container.get_parameters()

        return replace_all_placeholders(decorator_argument, matches, parameters, decorator_argument)

    def __check_type(self, value, expected_type: AbstractType, argument_name: str):
        value_type_str = value.__class__.__module__ + "." + value.__class__.__name__
        expected_type_str = str(expected_type)

        if expected_type.is_defined() and value_type_str != expected_type_str:
            expected_type_str = expected_type_str.replace("builtins.", "")
            value_type_str = value_type_str.replace("builtins.", "")
            raise Exception(f'Argument "{argument_name}" is defined as {expected_type_str}, {value_type_str} given instead')

        return value
