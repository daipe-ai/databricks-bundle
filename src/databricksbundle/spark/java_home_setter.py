from pathlib import Path
import os


def set_java_home():
    databricks_java_path = Path.home().joinpath(".databricks-connect-java")

    if not databricks_java_path.exists():
        raise Exception(f"Databricks Java (JDK) path {databricks_java_path} does not exist")

    os.environ["JAVA_HOME"] = str(databricks_java_path)
