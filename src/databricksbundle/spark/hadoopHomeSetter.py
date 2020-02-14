from pathlib import Path
import os

def setHadoopHomeEnvVar():
    if 'CONDA_PREFIX' not in os.environ:
        raise Exception('CONDA_PREFIX environment variable not set')

    venvPath = os.environ['CONDA_PREFIX']

    hadoopHomePath = Path(Path(venvPath).joinpath('hadoop'))

    os.environ['HADOOP_HOME'] = str(hadoopHomePath)
