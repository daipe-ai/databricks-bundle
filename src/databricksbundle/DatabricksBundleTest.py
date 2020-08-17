import unittest
from injecta.testing.servicesTester import testServices
from injecta.config.YamlConfigReader import YamlConfigReader
from injecta.package.pathResolver import resolvePath
from typing import List
from pyfony.kernel.BaseKernel import BaseKernel
from pyfonybundles.Bundle import Bundle
from databricksbundle.DatabricksBundle import DatabricksBundle

class DatabricksBundleTest(unittest.TestCase):

    def test_azure(self):
        container = self.__createContainer('test_azure')

        testServices(container)

    def test_aws(self):
        container = self.__createContainer('test_aws')

        testServices(container)

    def test_test(self):
        container = self.__createContainer('test_test')

        testServices(container)

    def __createContainer(self, appEnv: str):
        class Kernel(BaseKernel):

            _allowedEnvironments = ['test_aws', 'test_azure', 'test_test']

            def _registerBundles(self) -> List[Bundle]:
                return [
                    DatabricksBundle('spark_test.yaml')
                ]

        kernel = Kernel(
            appEnv,
            resolvePath('databricksbundle') + '/_config',
            YamlConfigReader()
        )

        return kernel.initContainer()

if __name__ == '__main__':
    unittest.main()
