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
        logger = logging.getLogger('test_logger')

        self.__argumentResolver = ArgumentResolver(logger, self.__createDummyContainer())

    def test_explicitIntArgument(self):
        inspectedArgument = InspectedArgument('myVar', DType('builtins', 'int'))

        resolvedArgument = self.__argumentResolver.resolve(inspectedArgument, 123)

        self.assertEqual(123, resolvedArgument)

    def test_plainStrArgument(self):
        inspectedArgument = InspectedArgument('myVar', DType('builtins', 'str'))

        resolvedArgument = self.__argumentResolver.resolve(inspectedArgument, 'Hello')

        self.assertEqual('Hello', resolvedArgument)

    def test_strArgumentWithPlaceholders(self):
        inspectedArgument = InspectedArgument('myVar', DType('builtins', 'str'), 'Some default hello', True)

        resolvedArgument = self.__argumentResolver.resolve(inspectedArgument, 'Hello %name% %surname%')

        self.assertEqual('Hello Peter Novak', resolvedArgument)

    def test_strArgumentService(self):
        inspectedArgument = InspectedArgument('myVar', DType(ScriptSessionFactory.__module__, 'ScriptSessionFactory'))

        resolvedSparkSessionFactory = self.__argumentResolver.resolve(inspectedArgument, f'@{ScriptSessionFactory.__module__}')

        self.assertIsInstance(resolvedSparkSessionFactory, ScriptSessionFactory)

    def test_argumentWithDefaultValue(self):
        inspectedArgument = InspectedArgument('myVar', DType('builtins', 'str'), 'Peter', True)

        resolvedArgument = self.__argumentResolver.resolve(inspectedArgument, None)

        self.assertEqual('Peter', resolvedArgument)

    def test_noValueNoTypehint(self):
        inspectedArgument = InspectedArgument('myVar', DType('inspect', '_empty'))

        with self.assertRaises(Exception) as error:
            self.__argumentResolver.resolve(inspectedArgument, None)

        self.assertEqual('Argument "myVar" must either have explicit value, default value or typehint defined', str(error.exception))

    def test_logger(self):
        inspectedArgument = InspectedArgument('myLogger', DType('logging', 'Logger'))

        resolvedLogger = self.__argumentResolver.resolve(inspectedArgument, None)

        self.assertIsInstance(resolvedLogger, logging.Logger)
        self.assertEqual('test_logger', resolvedLogger.name)

    def test_generalService(self):
        inspectedArgument = InspectedArgument('sparkSessionFactory', DType(ScriptSessionFactory.__module__, 'ScriptSessionFactory'))

        resolvedSparkSessionFactory = self.__argumentResolver.resolve(inspectedArgument, None)

        self.assertIsInstance(resolvedSparkSessionFactory, ScriptSessionFactory)

    def __createDummyContainer(self):
        class DummyContainer(ContainerInterface):

            def getParameters(self) -> Box:
                return Box({
                    'name': 'Peter',
                    'surname': 'Novak'
                })

            def get(self, ident):
                if ident == ScriptSessionFactory.__module__ or ident.__module__ == ScriptSessionFactory.__module__:
                    return ScriptSessionFactory()

                raise Exception(f'Unexpected service {ident}')

        return DummyContainer()

if __name__ == '__main__':
    unittest.main()
