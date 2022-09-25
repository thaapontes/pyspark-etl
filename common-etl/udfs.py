from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

'''
Useful links
------------
On how to create udfs : https://sparkbyexamples.com/pyspark/pyspark-udf-user-defined-function/

Instructions
------------
Avoid creating udf since they are black boxes for spark catalyst
'''


def upper_case(string: str):
    return string.upper()


upper_case_udf = udf(lambda z: upper_case(z), StringType())


def register_udf(spark: SparkSession):
    spark.udf.register("upper_case_udf", upper_case, StringType())
