# pylint: disable = invalid-name
from databricksbundle.notebook.decorator.StringableParameterInterface import StringableParameterInterface

class tableParams(StringableParameterInterface):

    def __init__(self, identifier: str, paramPathParts: list = None):
        self._identifier = identifier
        self._paramPathParts = paramPathParts if paramPathParts else []

    def __getattr__(self, item):
        paramPathParts = self._paramPathParts.copy()
        paramPathParts.append(item)

        return tableParams(self._identifier, paramPathParts)

    def toString(self):
        basePath = f'datalakebundle.tables."{self._identifier}".params'

        if self._paramPathParts:
            return '%' + basePath + '.' + '.'.join(self._paramPathParts) + '%'

        return '%' + basePath + '%'
