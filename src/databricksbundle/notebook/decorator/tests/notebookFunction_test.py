# pylint: disable = all
import os
from databricksbundle.notebook.decorators import notebookFunction

os.environ['APP_ENV'] = 'test_azure'

@notebookFunction()
def load_data():
    return 155

@notebookFunction()
def load_data2():
    pass

@notebookFunction(load_data, load_data2)
def rename_columns(police_number: int, something):
    assert police_number == 155
    assert something is None

if __name__ == '__main__':
    assert isinstance(load_data, notebookFunction)
    assert isinstance(load_data2, notebookFunction)
    assert isinstance(rename_columns, notebookFunction)
