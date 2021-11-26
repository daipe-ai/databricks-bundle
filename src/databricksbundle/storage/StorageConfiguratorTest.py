import unittest
from pyfonycore.bootstrap import bootstrapped_container
from databricksbundle.storage.StorageConfigurator import StorageConfigurator
from databricksbundle.storage.testing.SparkSessionMock import SparkSessionMock


class StorageConfiguratorTest(unittest.TestCase):
    def test_azure(self):
        container = bootstrapped_container.init("test_azure")
        storage_configurator: StorageConfigurator = container.get(StorageConfigurator)

        spark_mock = SparkSessionMock()

        storage_configurator.configure(spark_mock)

        self.assertEqual("tenant/123456", spark_mock.conf.get("testing.storage.aaa"))
        self.assertEqual("secrets/some_client_id_scope1/some_client_id_key1", spark_mock.conf.get("testing.secrets.aaa.client_id"))
        self.assertEqual(
            "secrets/some_client_secret_scope1/some_client_secret_key1", spark_mock.conf.get("testing.secrets.aaa.client_secret")
        )

        self.assertEqual("tenant/987654", spark_mock.conf.get("testing.storage.bbb"))
        self.assertEqual("secrets/some_client_id_scope2/some_client_id_key2", spark_mock.conf.get("testing.secrets.bbb.client_id"))
        self.assertEqual(
            "secrets/some_client_secret_scope2/some_client_secret_key2", spark_mock.conf.get("testing.secrets.bbb.client_secret")
        )


if __name__ == "__main__":
    unittest.main()
