# Pyspark DataSet Wrapper

A minimum pyspark dataset wrapper to get rid of the tedious work using `withcolumn` and `udf` to 
create custom columns.

## Requirements

- Python: > 3.4.1
- Pyspark: > 2.1.0

## Install
1. `python setup.py`
2. `pip install pyspark-dataset`
3.  Just copy dataset.py into your project directory, and you are good to go


## Usuage
You can create a `DataSet` object from an pyspark `DataFrame` by using:

```python
myds = DataSet(mydf)
```
Then, you are safe to replace all `mydf` by `myds`.
The created `DataSet` has all the methods of pyspark `DataFrame`, like `show`, `describe`, `groupby`, `pivot`, `join`, etc with
extra functionalites of creating custom columns using `pandas`-style.

For example:
```python
myds["col1"] = myds["col1"].cast(T.IntegerType())
myds["col2"] = myds.col2.cast(T.IntegerType())
myds.col1_add_10 = myds.col1 + 10
myds.col1_add_20 = myds["Clicks"] + 20
myds["col1_x2"] = myds["col1"]*2
myds["col1_mod_2"] = myds["col1"]%2
myds.select("col1", "col2", "col1_add_10", "col1_add_20", "col1_x2", "col1_mod_2").show()
```

Moreover, you can even using a python function/lambda to create an column, the argument passed to your
custom function/lambda is a dict represents a `Row` of your dataframe.
```python
myds["max"] = lambda df: max(df["col1"], df["col2"])
myds["col1_digits_length"] = lambda df: len(str(df["col1"]))
mydf["col1_digits_length_map"] = lambda df: {str(df["col1"]): df["col1"]}

def custom_max(x, y, factor):
  return max(x, y*factor)

myds["custom_max"] = lambda df: custom_max(df["col1"], df["col2"], 5)
```
There is no need to declare the `ReturnType` for your custom function/lambda as in `pyspark udf`.
This DataSet wrappper will try to infer the return type for you.
In most case, it will work perfectly. In case failing to infer the return type, you can write like this:
```python
from pyspark.sql import types as T
myds["my_custom_col"] = lambda df: func(df), T.ArrayType(T.IntegerType())
```

This way of creating columns works also with any instance of `pyspark.sql.Column`, for example:
you can create a column using the windows function:
```python
from pyspark.sql import Window
myds["tot_clicks"] = F.sum("clicks").over(Window.partitionBy("site"))
myds.select("site", "tot_clicks").show(10)
```
