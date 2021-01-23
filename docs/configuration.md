## Configuring notebook functions

Configuration values defined in your app configuration can be simply passed into the notebook functions using the `%path.to.config%` notation: 

```python
from logging import Logger
from databricksbundle.notebook.decorators import notebookFunction

@notebookFunction('%testdata.path%')
def customers_table(testDataPath: str, logger: Logger):
    logger.info(f'Test data path: {testDataPath}')
```

Next section: [Databricks Connect setup](databricks-connect.md)
