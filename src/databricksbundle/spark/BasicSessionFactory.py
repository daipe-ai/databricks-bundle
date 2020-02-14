from pyspark.sql import SparkSession
from databricksbundle.spark.SparkSessionLazy import SparkSessionLazy

class BasicSessionFactory:

    def create(self) -> SparkSessionLazy:
        def createLazy():
            return SparkSession \
                .builder \
                .getOrCreate()

        return SparkSessionLazy(createLazy)
