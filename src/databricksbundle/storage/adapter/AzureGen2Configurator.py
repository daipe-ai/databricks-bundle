from box import Box
from pyspark.sql.session import SparkSession
from pyspark.dbutils import DBUtils
from databricksbundle.storage.StorageConfiguratorInterface import StorageConfiguratorInterface


class AzureGen2Configurator(StorageConfiguratorInterface):
    def configure(self, spark: SparkSession, config: Box):
        # initialized manually to prevent issues with Databricks Connect initialization
        dbutils = DBUtils(spark)

        spark.conf.set(
            f"fs.azure.account.oauth2.client.endpoint.{config.storage_name}.dfs.core.windows.net",
            f"https://login.microsoftonline.com/{config.tenant_id}/oauth2/token",
        )
        spark.conf.set(
            f"fs.azure.account.oauth.provider.type.{config.storage_name}.dfs.core.windows.net",
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        )
        spark.conf.set(f"fs.azure.account.auth.type.{config.storage_name}.dfs.core.windows.net", "OAuth")
        spark.conf.set(
            f"fs.azure.account.oauth2.client.id.{config.storage_name}.dfs.core.windows.net",
            dbutils.secrets.get(scope=config.client_id.secret_scope, key=config.client_id.secret_key),
        )
        spark.conf.set(
            f"fs.azure.account.oauth2.client.secret.{config.storage_name}.dfs.core.windows.net",
            dbutils.secrets.get(scope=config.client_secret.secret_scope, key=config.client_secret.secret_key),
        )

    def get_type(self):
        return "azure_gen2"
