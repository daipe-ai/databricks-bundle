import os


def clean_existing_spark_config():
    if "SPARK_HOME" in os.environ:
        del os.environ["SPARK_HOME"]
