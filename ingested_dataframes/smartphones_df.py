from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

"""
    Dataframe from array
"""

__all__ = ["smartphones_dataframe"]


def smartphones_dataframe(spark: SparkSession):
    data_array = [["Apple", "iPhone X", "IOS", 46], ["Xiaomi", "Mi 9", "Android", 54]]
    schema = StructType([StructField('make', StringType(), True),
                         StructField('model', StringType(), True),
                         StructField('platform', IntegerType(), True),
                         StructField('camera_megapixels', IntegerType(), True)])
    df_from_array = spark.createDataFrame(data_array, schema)
    print("Smartphone DataFrame: ")
    df_from_array.show(5)

    return df_from_array
