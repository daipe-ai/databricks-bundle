## Writing function-based notebooks

The databricks-bundle defines the very basic `@notebookFunction` decorator which can be used to chain calls of notebook functions in the correct order: 

![alt text](./notebook-functions.png)

Once you run the `active_customers_only` function's cell, it gets is automatically called with the dataframe loaded by the `customers_table` function.

Similarly, once you run the `save_output` function's cell, it gets automatically called with the filtered dataframe returned from the `active_customers_only` function.

___

Next section: [Using pre-configured objects](dependencies.md)
