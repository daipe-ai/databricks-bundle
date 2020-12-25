## Configuring notebook functions

Configuration values defined in your app configuration can be simply passed into the notebook functions using the `%path.to.config%` notation: 

```python
from logging import Logger
from databricksbundle.notebook.decorators import notebookFunction

@notebookFunction('%testdata.path%')
def customers_table(testDataPath: str, logger: Logger):
    logger.info(f'Test data path: {testDataPath}')
```

### Using table-specific configuration

If you use the [datalake-bundle](https://github.com/bricksflow/datalake-bundle), you can also define table level parameters, which can be passed to any notebook function:

```yaml
parameters:
  datalakebundle:
    tables:
      customer.my_table:
        schemaPath: 'myapp.client.schema'
        targetPath: '/data/clients.delta'
        params:
          testDataPath: '/foo/bar'
```

Code of the **customer/my_table.py** notebook:

```python
from logging import Logger
from databricksbundle.notebook.decorators import notebookFunction, tableParams

@notebookFunction(tableParams('customer.my_table').testDataPath)
def customers_table(testDataPath: str, logger: Logger):
    logger.info(f'Test data path: {testDataPath}')
```

Next section: [Databricks Connect setup](databricks-connect.md)
