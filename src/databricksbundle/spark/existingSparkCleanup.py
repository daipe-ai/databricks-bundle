import os

def cleanExistingSparkConfig():
    if 'SPARK_HOME' in os.environ:
        del os.environ['SPARK_HOME']
