## Configuring notebook functions

Configuration values defined in your app configuration can be simply passed into the notebook functions using the `%path.to.config%` notation: 

Example `config.yaml` configuration:

```yaml
parameters:
  csvdata:
    path: '/data/csv
```

Example notebook code:

```python
from logging import Logger
from databricksbundle.notebook.decorator.notebookFunction import notebookFunction

@notebookFunction('%csvdata.path%')
def customers_table(csvDataPath: str, logger: Logger):
    logger.info(f'CSV data path: {csvDataPath}')
```

Next section: [Databricks Connect setup](databricks-connect.md)
