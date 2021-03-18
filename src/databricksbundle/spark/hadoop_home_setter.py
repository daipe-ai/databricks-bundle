from pathlib import Path
import os


def set_hadoop_home_env_var():
    venv_path = os.getcwd() + "/.venv"

    hadoop_home_path = Path(Path(venv_path).joinpath("hadoop"))

    os.environ["HADOOP_HOME"] = str(hadoop_home_path)
