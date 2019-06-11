from collections import Mapping, Callable
from functools import wraps

from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Column, GroupedData


class AttributeAccessProbe(object):
    def __init__(self, row):
        self.attrs = []
        self.row = row

    def __getattr__(self, key):
        if key not in ["row", "attrs"]:
            self.attrs.append(key)
            return self.row[key]
        else:
            super(AttributeAccessProbe, self).__getattr__(key)

    def __getitem__(self, key):
        self.attrs.append(key)
        return self.row[key]


class DataSet(object):
    __slots__ = ["_df"]

    def __init__(self, df, checkpoint=False):
        if not isinstance(df, DataFrame):
            raise Exception(
                "The delegate variable is not a pyspark dataframe!")

        self._df = df

        if checkpoint:
            self._df.persist()
            self._df = self._df.checkpoint()

    def __setattr__(self, key, value):
        if key == "_df":
            super(DataSet, self).__setattr__(key, value)
        else:
            self._create_cols(key, value)

    def __getattr__(self, key):
        if key == "_df":
            return super(DataSet, self).__getattr__(key)
        else:
            return getattr(self._df, key)

    def __getitem__(self, key):
        return self.__getattr__(key)

    def __setitem__(self, key, value):
        self.__setattr__(key, value)

    @staticmethod
    def get_ret_type(value):
        if isinstance(value, float):
            return T.FloatType()
        if isinstance(value, int):
            return T.IntegerType()
        if isinstance(value, str):
            return T.StringType()
        if isinstance(value, list):
            if len(value) == 0:
                raise Exception(
                    "Python Dataset Wrapper: Failed to parser return type for list"
                )
            return T.ArrayType(DataSet.get_ret_type(value[0]))
        if isinstance(value, Mapping):
            if len(value) == 0:
                raise Exception(
                    "Python Dataset Wrapper: Failed to parser return type for dict"
                )
            x, y = value.popitem()
            return T.MapType(DataSet.get_ret_type(x), DataSet.get_ret_type(y))

    def get_attributes(self, func, get_ret_type=True):
        try:
            probe_obj = AttributeAccessProbe(self._df.take(1)[0])
            ret = func(probe_obj)
        except ValueError as e:
            missing_col = e.message
            raise ValueError(
                "Column {} not exist in dataframe".format(missing_col))
        if get_ret_type:
            return probe_obj.attrs, DataSet.get_ret_type(ret)
        else:
            return probe_obj.attrs, None

    def _create_cols(self, col, func):
        if isinstance(func, Callable):
            all_attrs, return_type = self.get_attributes(func)
            myudf = F.udf(func, return_type)
            self._df = self._df.withColumn(col, myudf(F.struct(*all_attrs)))
        elif isinstance(func, Column):
            self._df = self._df.withColumn(col, func)
        elif isinstance(func, float) or isinstance(func, int) or isinstance(
                func, str) or isinstance(func, bool):
            self._df = self._df.withColumn(col, F.lit(func))
        elif isinstance(func, tuple):
            t_func, t_ret_type = func[0], func[1]
            if not isinstance(t_func, Callable):
                raise NotImplementedError(
                    "Way using {} to create columns is not implemented".format(
                        type(func)))
            all_attrs, _ = self.get_attributes(t_func, False)
            myudf = F.udf(t_func, t_ret_type)
            self._df = self._df.withColumn(col, myudf(F.struct(*all_attrs)))
        else:
            raise NotImplementedError(
                "Way using {} to create columns is not implemented".format(
                    type(func)))

    def rename(self, data):
        if isinstance(data, tuple):
            self._df = self._df.withColumnRenamed(data[0], data[1])
        elif isinstance(data, dict):
            for old_col, new_col in data.items():
                self._df = self._df.withColumnRenamed(old_col, new_col)
        else:
            raise ValueError(
                "Wrong argument type for renaming dataframe columns")

    @property
    def df(self):
        return self._df


class DataSetGroupedData(object):

    __slots__ = ["_data"]

    def __init__(self, data):
        self._data = data

    def __getattr__(self, key):
        if key == "_data":
            return super(DataSetGroupedData, self).__getattr__(key)
        else:
            return getattr(self._data, key)

    @property
    def data(self):
        return self._data


def grouped_data_delegate_generator(func):
    @wraps(func)
    def _wrapper(self, *args, **kwargs):
        delegate_func = getattr(self._data, func.__name__)
        ret = delegate_func(*args, **kwargs)
        if isinstance(ret, GroupedData):
            return DataSetGroupedData(ret)
        if isinstance(ret, DataFrame):
            return DataSet(ret)
        return ret

    return _wrapper


# monkey-patch to create delegate methods for our DataSetGroupedData Wrapper
for func_name in dir(GroupedData):
    func = getattr(GroupedData, func_name)
    if not func_name.startswith("_") and isinstance(func, Callable):
        setattr(DataSetGroupedData, func_name,
                grouped_data_delegate_generator(func))


def dataframe_delegate_generator(func):
    @wraps(func)
    def _wrapper(self, *args, **kwargs):
        delegate_func = getattr(self._df, func.__name__)
        # degrade params
        new_args = []
        for i in range(len(args)):
            if isinstance(args[i], DataSet):
                new_args.append(args[i].df)
            else:
                new_args.append(args[i])
        for key, value in kwargs.items():
            if isinstance(value, DataSet):
                kwargs[key] = value.df
        # then execute the delegate dataframe method
        ret = delegate_func(*new_args, **kwargs)
        if isinstance(ret, DataFrame):
            return DataSet(ret)
        if isinstance(ret, GroupedData):
            return DataSetGroupedData(ret)
        return ret

    return _wrapper


# monkey-patch to create delegate methods for our DataSet Wrapper
for func_name in dir(DataFrame):
    func = getattr(DataFrame, func_name)
    if not func_name.startswith("_") and isinstance(func, Callable):
        setattr(DataSet, func_name, dataframe_delegate_generator(func))
