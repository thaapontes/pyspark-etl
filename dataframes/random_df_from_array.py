from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, StructField

"""
    Dataframe from array
"""

__all__ = ["create_dataframe_from_array"]


def create_dataframe_from_array(sc: SparkSession):
    data_array = [[1], [2], [3], [4]]
    schema = StructType([StructField('age', IntegerType(), True)])
    df_from_array = sc.createDataFrame(data_array, schema)
    df_from_array.show(5)
