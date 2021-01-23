## Using pre-configured objects

Notebook functions can be injected with objects defined in the app:

```python
from databricksbundle.notebook.decorator.notebookFunction import notebookFunction
from logging import Logger
from pyspark.sql.session import SparkSession

@notebookFunction()
def customers_table(spark: SparkSession, logger: Logger):
    logger.info('Reading my_crm.customers')

    return spark.read.table('my_crm.customers')
```

The common objects that can be injected are:

* `spark: SparkSession` (`from pyspark.sql.session import SparkSession`)  
The Databricks spark instance itself.

* `dbutils: DBUtils` (`from pyspark.dbutils import DBUtils`)  
[Databricks utilities object](https://docs.databricks.com/dev-tools/databricks-utils.html).

* `logger: Logger` (`from logging import Logger`)  
Logger instance for the given notebook.

### (Expert) Passing explicitly defined services into notebook functions

Services, which cannot be autowired (= classes with multiple instances), can be injected into the notebook functions explicitly using the `@serviceName` notation:

```python
from databricksbundle.notebook.decorator.notebookFunction import notebookFunction

@notebookFunction('@my.service')
def customers_table(myService: MyClass):
    myService.doSomething()
```

See [Injecta](https://github.com/pyfony/injecta)'s documentation for more details on the syntax.

___

Next section: [Configuring notebook functions](configuration.md)
