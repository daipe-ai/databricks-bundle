## Writing function-based notebooks

This bundle allows you to write function-based notebooks, that provides the same user-experience as if you code without any functions.
Just write the function, annotate it with a proper decorator and run the cell.  

There are **4 types of notebook functions**:
 
`@dataFrameLoader` - loads some Spark dataframe (from Hive table, csv, ...) and returns it

![alt text](./dataFrameLoader.png)

`@transformation` - transforms given dataframe(s) (filter, JOINing, grouping, ...) and returns the result

![alt text](./transformation.png)

`@dataFrameSaver` - saves given dataframe into some permanent storage (parquet, Delta, csv, ...)

![alt text](./dataFrameSaver.png)

`@notebookFunction` - general notebook function that may contain any arbitrary code; it gets automatically invoked once you run the notebook cell 

![alt text](./notebookFunction.png)

___

Next section: [Recommended notebooks structure](structure.md)
