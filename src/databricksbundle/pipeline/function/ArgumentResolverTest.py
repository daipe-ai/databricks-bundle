import logging
import unittest
from pathlib import Path
from box import Box
from injecta.container.ContainerInterface import ContainerInterface
from injecta.dtype.DType import DType
from injecta.service.class_.InspectedArgument import InspectedArgument
from databricksbundle.pipeline.function.ArgumentResolver import ArgumentResolver
from databricksbundle.pipeline.function.service.ServiceResolverInterface import ServiceResolverInterface
from databricksbundle.spark.ScriptSessionFactory import ScriptSessionFactory

class ArgumentResolverTest(unittest.TestCase):

    def setUp(self):
        serviceResolvers = {
            'logging.Logger': '@databricksbundle.test.DummyLoggerResolver'
        }

        self.__argumentResolver = ArgumentResolver(serviceResolvers, self.__createDummyContainer())

    def test_explicitIntArgument(self):
        inspectedArgument = InspectedArgument('myVar', DType('builtins', 'int'))

        resolvedArgument = self.__argumentResolver.resolve(inspectedArgument, 123, Path('.'))

        self.assertEqual(123, resolvedArgument)

    def test_plainStrArgument(self):
        inspectedArgument = InspectedArgument('myVar', DType('builtins', 'str'))

        resolvedArgument = self.__argumentResolver.resolve(inspectedArgument, 'Hello', Path('.'))

        self.assertEqual('Hello', resolvedArgument)

    def test_strArgumentWithPlaceholders(self):
        inspectedArgument = InspectedArgument('myVar', DType('builtins', 'str'), 'Some default hello', True)

        resolvedArgument = self.__argumentResolver.resolve(inspectedArgument, 'Hello %name% %surname%', Path('.'))

        self.assertEqual('Hello Peter Novak', resolvedArgument)

    def test_strArgumentService(self):
        inspectedArgument = InspectedArgument('myVar', DType(ScriptSessionFactory.__module__, 'ScriptSessionFactory'))

        resolvedSparkSessionFactory = self.__argumentResolver.resolve(inspectedArgument, f'@{ScriptSessionFactory.__module__}', Path('.'))

        self.assertIsInstance(resolvedSparkSessionFactory, ScriptSessionFactory)

    def test_argumentWithDefaultValue(self):
        inspectedArgument = InspectedArgument('myVar', DType('builtins', 'str'), 'Peter', True)

        resolvedArgument = self.__argumentResolver.resolve(inspectedArgument, None, Path('.'))

        self.assertEqual('Peter', resolvedArgument)

    def test_noValueNoTypehint(self):
        inspectedArgument = InspectedArgument('myVar', DType('inspect', '_empty'))

        with self.assertRaises(Exception) as error:
            self.__argumentResolver.resolve(inspectedArgument, None, Path('.'))

        self.assertEqual('Argument "myVar" must either have explicit value, default value or typehint defined', str(error.exception))

    def test_mappedService(self):
        inspectedArgument = InspectedArgument('myLogger', DType('logging', 'Logger'))

        resolvedLogger = self.__argumentResolver.resolve(inspectedArgument, None, Path('.'))

        self.assertIsInstance(resolvedLogger, logging.Logger)
        self.assertEqual('test_logger', resolvedLogger.name)

    def test_generalService(self):
        inspectedArgument = InspectedArgument('sparkSessionFactory', DType(ScriptSessionFactory.__module__, 'ScriptSessionFactory'))

        resolvedSparkSessionFactory = self.__argumentResolver.resolve(inspectedArgument, None, Path('.'))

        self.assertIsInstance(resolvedSparkSessionFactory, ScriptSessionFactory)

    def __createDummyContainer(self):
        class DummyLoggerResolver(ServiceResolverInterface):

            def resolve(self, pipelinePath: Path) -> object:
                return logging.getLogger('test_logger')

        class DummyContainer(ContainerInterface):

            def getParameters(self) -> Box:
                return Box({
                    'name': 'Peter',
                    'surname': 'Novak'
                })

            def get(self, ident):
                if ident == 'databricksbundle.test.DummyLoggerResolver':
                    return DummyLoggerResolver()

                if ident == ScriptSessionFactory.__module__ or ident.__module__ == ScriptSessionFactory.__module__:
                    return ScriptSessionFactory()

                raise Exception(f'Unexpected service {ident}')

        return DummyContainer()

if __name__ == '__main__':
    unittest.main()
