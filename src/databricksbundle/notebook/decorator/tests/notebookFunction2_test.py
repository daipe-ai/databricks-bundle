# pylint: disable = all
from databricksbundle.notebook.decorators import notebookFunction

try:
    @notebookFunction
    def load_data():
        return 155
except Exception as e:
    assert str(e) == 'Use @notebookFunction() instead of @notebookFunction please'
