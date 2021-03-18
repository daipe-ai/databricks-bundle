import logging
import unittest
from box import Box
from injecta.container.ContainerInterface import ContainerInterface
from injecta.dtype.DType import DType
from injecta.service.class_.InspectedArgument import InspectedArgument
from databricksbundle.notebook.function.ArgumentResolver import ArgumentResolver
from databricksbundle.spark.ScriptSessionFactory import ScriptSessionFactory


class ArgumentResolverTest(unittest.TestCase):
    def setUp(self):
        logger = logging.getLogger("test_logger")

        self.__argument_resolver = ArgumentResolver(logger, self.__create_dummy_container())

    def test_explicit_int_argument(self):
        inspected_argument = InspectedArgument("my_var", DType("builtins", "int"))

        resolved_argument = self.__argument_resolver.resolve(inspected_argument, 123)

        self.assertEqual(123, resolved_argument)

    def test_plain_str_argument(self):
        inspected_argument = InspectedArgument("my_var", DType("builtins", "str"))

        resolved_argument = self.__argument_resolver.resolve(inspected_argument, "Hello")

        self.assertEqual("Hello", resolved_argument)

    def test_str_argument_with_placeholders(self):
        inspected_argument = InspectedArgument("my_var", DType("builtins", "str"), "Some default hello", True)

        resolved_argument = self.__argument_resolver.resolve(inspected_argument, "Hello %name% %surname%")

        self.assertEqual("Hello Peter Novak", resolved_argument)

    def test_str_argument_service(self):
        inspected_argument = InspectedArgument("my_var", DType(ScriptSessionFactory.__module__, "ScriptSessionFactory"))

        resolved_spark_session_factory = self.__argument_resolver.resolve(inspected_argument, f"@{ScriptSessionFactory.__module__}")

        self.assertIsInstance(resolved_spark_session_factory, ScriptSessionFactory)

    def test_argument_with_default_value(self):
        inspected_argument = InspectedArgument("my_var", DType("builtins", "str"), "Peter", True)

        resolved_argument = self.__argument_resolver.resolve(inspected_argument, None)

        self.assertEqual("Peter", resolved_argument)

    def test_no_value_no_typehint(self):
        inspected_argument = InspectedArgument("my_var", DType("inspect", "_empty"))

        with self.assertRaises(Exception) as error:
            self.__argument_resolver.resolve(inspected_argument, None)

        self.assertEqual('Argument "my_var" must either have explicit value, default value or typehint defined', str(error.exception))

    def test_logger(self):
        inspected_argument = InspectedArgument("my_logger", DType("logging", "Logger"))

        resolved_logger = self.__argument_resolver.resolve(inspected_argument, None)

        self.assertIsInstance(resolved_logger, logging.Logger)
        self.assertEqual("test_logger", resolved_logger.name)

    def test_general_service(self):
        inspected_argument = InspectedArgument("spark_session_factory", DType(ScriptSessionFactory.__module__, "ScriptSessionFactory"))

        resolved_spark_session_factory = self.__argument_resolver.resolve(inspected_argument, None)

        self.assertIsInstance(resolved_spark_session_factory, ScriptSessionFactory)

    def __create_dummy_container(self):
        class DummyContainer(ContainerInterface):
            def get_parameters(self) -> Box:
                return Box({"name": "Peter", "surname": "Novak"})

            def get(self, ident):
                if ident == ScriptSessionFactory.__module__ or ident.__module__ == ScriptSessionFactory.__module__:
                    return ScriptSessionFactory()

                raise Exception(f"Unexpected service {ident}")

        return DummyContainer()


if __name__ == "__main__":
    unittest.main()
