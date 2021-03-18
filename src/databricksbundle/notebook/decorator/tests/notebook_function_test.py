import os
from databricksbundle.notebook.decorator.notebook_function import notebook_function

os.environ["APP_ENV"] = "test_azure"


@notebook_function()
def load_data():
    return 155


@notebook_function()
def load_data2():
    pass


@notebook_function(load_data, load_data2)
def rename_columns(police_number: int, something):
    assert police_number == 155
    assert something is None


if __name__ == "__main__":
    assert isinstance(load_data, notebook_function)
    assert load_data.result == 155
    assert isinstance(load_data2, notebook_function)
    assert load_data2.result is None
    assert isinstance(rename_columns, notebook_function)
    assert rename_columns.result is None
