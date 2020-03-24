from pathlib import Path
import os

def setJavaHome():
    databricksJavaPath = Path.home().joinpath('.databricks-connect-java')

    if not databricksJavaPath.exists():
        raise Exception(f'Databricks Java (JDK) path {databricksJavaPath} does not exist')

    os.environ['JAVA_HOME'] = str(databricksJavaPath)
