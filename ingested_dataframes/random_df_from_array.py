from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, StructField

"""
    Dataframe from array
"""

__all__ = ["create_dataframe_from_array"]


def create_dataframe_from_array(spark: SparkSession):
    data_array = [[1], [2], [3], [4]]
    schema = StructType([StructField('age', IntegerType(), True)])
    df_from_array = spark.createDataFrame(data_array, schema)
    print("Array DataFrame: ")
    df_from_array.show(5)

    return df_from_array
