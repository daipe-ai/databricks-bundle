import unittest
from pyfonycore.bootstrap import bootstrapped_container
from injecta.testing.services_tester import test_services


class DatabricksBundleTest(unittest.TestCase):
    def test_azure(self):
        container = bootstrapped_container.init("test_azure")

        test_services(container)

    def test_aws(self):
        container = bootstrapped_container.init("test_aws")

        test_services(container)


if __name__ == "__main__":
    unittest.main()
