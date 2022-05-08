from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, StructField, StructType, DoubleType, LongType

"""
    Csv example from: https://www.kaggle.com/datasets
    Small file: KB
"""

__all__ = ["spotify_songs"]


# INGESTION WITH SCHEMA
def spotify_songs(spark: SparkSession):
    data_file_path = '/Users/thabata.pontes/Desktop/PySparkETL/kaggle_files/top2018.csv'

    # types must be right in order to data be ingested
    schema = StructType([StructField('id', StringType(), False),
                         StructField('name', StringType()),
                         StructField('artists', StringType()),
                         StructField('danceability', DoubleType())])

    df = spark.read.format("csv").option("header", "true").schema(schema).load(data_file_path)
    print("Spotify songs DataFrame: ")
    df.show(5)

    return df
