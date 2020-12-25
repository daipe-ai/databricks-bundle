import unittest
from pyfonycore.bootstrap import bootstrappedContainer
from injecta.testing.servicesTester import testServices

class DatabricksBundleTest(unittest.TestCase):

    def test_azure(self):
        container = bootstrappedContainer.init('test_azure')

        testServices(container)

    def test_aws(self):
        container = bootstrappedContainer.init('test_aws')

        testServices(container)

if __name__ == '__main__':
    unittest.main()
