from pathlib import Path
import os

def setHadoopHomeEnvVar():
    venvPath = os.getcwd() + '/.venv'

    hadoopHomePath = Path(Path(venvPath).joinpath('hadoop'))

    os.environ['HADOOP_HOME'] = str(hadoopHomePath)
