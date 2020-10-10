## Configuring notebook functions

Configuration values defined in your app configuration can be simply passed into the notebook functions using the `%path.to.config%` notation: 

```python
from logging import Logger
from databricksbundle.notebook.decorators import notebookFunction

@notebookFunction('%testdata.path%')
def customers_table(testDataPath: str, logger: Logger):
    logger.info(f'Test data path: {testDataPath}')
```

### Using notebook-specific configuration

If you use the [datalake-bundle](https://github.com/bricksflow/datalake-bundle), you can also define notebook level parameters, which can be passed to each notebook function:

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
from box import Box
from logging import Logger
from databricksbundle.notebook.decorators import notebookFunction

@notebookFunction()
def customers_table(notebookParams: Box, logger: Logger):
    logger.info(f'Test data path: {notebookParams.testDataPath}')
``` 

___

Next section: [Databricks Connect setup](databricks-connect.md)
