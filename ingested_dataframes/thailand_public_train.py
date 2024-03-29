import os

from pyspark.sql import SparkSession

"""
    Csv example from: https://www.kaggle.com/datasets
    Small file: 12Kb
"""

__all__ = ["thailand_public"]


def thailand_public(spark: SparkSession):
    current_dir = "/Users/thabata.pontes/Desktop/PySparkETL/kaggle_files/"
    relative_path = "thailand_public_train.json"
    absolute_file_path = os.path.join(current_dir, relative_path)
    # JSON INGESTION WITHOUT SCHEMA
    df = spark.read.format("json").option("multiline", "true").load(absolute_file_path)
    # JSON INGESTION WITH SCHEMA
    # df = spark.read.format("json").option("inferSchema", "true").load(absolute_file_path)

    print("Thailand public train DataFrame: ")
    df.show(5)

    return df
