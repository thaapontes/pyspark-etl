import os

from pyspark.sql import SparkSession

__all__ = ["movies"]


# INGESTION WITHOUT SCHEMA
def movies(spark: SparkSession):
    current_dir = "/Users/thabata.pontes/Desktop/PySparkETL/kaggle_files/"
    relative_path = "movies.json"
    absolute_file_path = os.path.join(current_dir, relative_path)
    df = spark.read.option("inferSchema", "true").json(absolute_file_path)

    print("Movies DataFrame")
    df.show()

    return df
