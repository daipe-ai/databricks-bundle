import unittest
from injecta.testing.servicesTester import testServices
from injecta.config.YamlConfigReader import YamlConfigReader
from injecta.package.pathResolver import resolvePath
from typing import List
from pyfony.PyfonyBundle import PyfonyBundle
from pyfony.kernel.BaseKernel import BaseKernel
from pyfonybundles.Bundle import Bundle
from databricksbundle.DatabricksBundle import DatabricksBundle

class DatabricksBundleTest(unittest.TestCase):

    def test_init(self):
        class Kernel(BaseKernel):

            def _registerBundles(self) -> List[Bundle]:
                return [
                    PyfonyBundle(),
                    DatabricksBundle()
                ]

        kernel = Kernel(
            'test',
            resolvePath('databricksbundle') + '/_config',
            YamlConfigReader()
        )

        container = kernel.initContainer()

        testServices(container)

if __name__ == '__main__':
    unittest.main()
