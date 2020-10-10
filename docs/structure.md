## Recommended notebooks structure

In Bricksflow, it is recommended to divide your tables and notebooks into the the following layers:
 
* **bronze** - "staging layer", raw data from source systems
* **silver** - most business logic, one or multiple tables per use-case 
* **gold** - additional filtering/aggregations of silver data (using views or materialized tables) to be served to the final customers

For databases and tables in each of bronze/silver/gold layers it is recommended to follow the **[db_name/table_name]** directory structure.  

```yaml
src
    [PROJECT_NAME]
        bronze_db_batch
            tbl_customers
                tbl_customers.py # table creation code
                schema.py # output table schema definition
            tbl_products
                tbl_products.py
                schema.py
            tbl_contracts
                tbl_contracts.py
                schema.py
            ...
        silver_db_batch
            tbl_product_profitability
                tbl_product_profitability.py
                schema.py
            tbl_customer_profitability
                tbl_customer_profitability.py
                schema.py
            tbl_customer_onboarding
                tbl_customer_onboarding.py
                schema.py
            ...
        gold_db_batch
            vw_product_profitability # view on silver_db_batch.tbl_product_profitability
                vw_product_profitability.py
                schema.py
            tbl_customer_profitability # "materialized" view on silver_db_batch.tbl_customer_profitability
                tbl_customer_profitability.py
                schema.py
            vw_customer_onboarding
                vw_customer_onboarding.py
                schema.py
```

**Further notes:**

* Each table should have explicit schema defined (the *schema.py* file)
* Notebook python file (*tbl_product_profitability.py*) has the same name as the directory where it is stored (*tbl_product_profitability*)

___

Next section: [Using dependencies](dependencies.md)
