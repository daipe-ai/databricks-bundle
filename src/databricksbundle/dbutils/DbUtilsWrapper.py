import types
import platform
from databricksbundle.detector import is_databricks


class DbUtilsWrapper:
    def __init__(self, factory_callback: callable):
        self._factory_callback = factory_callback
        self._db_utils = None

    def __getattr__(self, attribute_name):
        if self._db_utils is None:
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

            self._db_utils = self._factory_callback()

        if hasattr(self._db_utils, attribute_name) is False:
            raise AttributeError(attribute_name)

        attr = getattr(self._db_utils, attribute_name)

        if isinstance(attr, types.FunctionType) is False:
            return attr

        def wrapper(*args, **kw):
            return attr(*args, **kw)

        return wrapper
