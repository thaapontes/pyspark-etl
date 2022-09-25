from pyspark.sql import SparkSession
# noinspection PyUnresolvedReferences
from delta import *

import ingested_dataframes
import transformed_dataframes
from udfs import register_udf

if __name__ == '__main__':

    '''
    Create a session on a local master
    To allow Databricks Delta Lake: https://docs.delta.io/latest/quick-start.html#pyspark-shell
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    '''
    spark = SparkSession.builder \
        .appName("reading csv") \
        .master("local") \
        .getOrCreate()

    '''
    Call udfs
    '''
    register_udf(spark)

    ''' 
    Call ingested dataframes
    Multiple ways to ingest different formats:
    https://github.com/jgperrin/net.jgp.books.spark.ch07/tree/master/src/main/python
    Reading a DF:
    - format: json, csv, text, parquet (default format in Spark, not needed to put .format)
    - enforce schema or inferSchema = true
    - path in load() or options
    - zero or more options like mode (failFast, dropMalformed, permissive), 
    dateFormat, header, sep, nullValue
    '''
    for dataframe in ingested_dataframes.__all__(spark):
        dataframe

    ''' 
    Writing a DF:
    - format: json, csv, text, parquet (default format in Spark, not needed to put .format)
    - mode: overwrite, append, ignore, errorIfExists
    - path in save()
    - zero or more options
    '''
    for dataframe in transformed_dataframes.__all__(spark):
        dataframe

    # Stop SparkSession at the end of the application
    spark.stop()
