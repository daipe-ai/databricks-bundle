# pylint: disable = invalid-name, not-callable
from databricksbundle.notebook.decorator.AbstractDecorator import AbstractDecorator
from databricksbundle.notebook.decorator.DecoratorMetaclass import DecoratorMetaclass

class notebookFunction(AbstractDecorator, metaclass=DecoratorMetaclass):

    # empty __init__() to suppress PyCharm's "unexpected arguments" error
    def __init__(self, *args):
        pass
