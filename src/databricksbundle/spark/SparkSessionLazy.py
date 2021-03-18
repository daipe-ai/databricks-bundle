import platform
from databricksbundle.detector import is_databricks


class SparkSessionLazy:
    def __init__(self, factory_callback: callable):
        self._factory_callback = factory_callback
        self._spark_session = None

    def __getattr__(self, attribute_name):
        if self._spark_session is None:
            if not is_databricks():
                from databricksbundle.spark.java_home_setter import set_java_home
                from databricksbundle.spark.existing_spark_cleanup import (
                    clean_existing_spark_config,
                )

                set_java_home()
                clean_existing_spark_config()

                if platform.system() == "Windows":
                    from databricksbundle.spark.hadoop_home_setter import (
                        set_hadoop_home_env_var,
                    )

                    set_hadoop_home_env_var()

            self._spark_session = self._factory_callback()

        return getattr(self._spark_session, attribute_name)
