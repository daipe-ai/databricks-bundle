## Configuring pipelines

Configuration values defined in your app configuration can be simply passed into the pipeline functions using the `%path.to.config%` notation: 

```python
from logging import Logger
from databricksbundle.pipeline.decorator.loader import pipelineFunction

@pipelineFunction('%testdata.path%')
def customers_table(testDataPath: str, logger: Logger):
    logger.info(f'Test data path: {testDataPath}')
```

### Using pipeline-specific configuration

If you use the [datalake-bundle](https://github.com/bricksflow/datalake-bundle), you can also define pipeline level parameters, which can be passed to each pipeline function:

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

Code of the **customer/my_table.py** pipeline:

```python
from box import Box
from logging import Logger
from databricksbundle.pipeline.decorator.loader import pipelineFunction

@pipelineFunction()
def customers_table(pipelineParams: Box, logger: Logger):
    logger.info(f'Test data path: {pipelineParams.testDataPath}')
``` 

___

Next section: [Databricks Connect setup](databricks-connect.md)
