from pyspark.sql import SparkSession

"""
    Csv example from: https://www.kaggle.com/datasets
    Medium file: 1GB
"""

__all__ = ["labels_covid"]


# INGESTION WITHOUT SCHEMA
def labels_covid(spark: SparkSession):
    data_file_path = '/Users/thabata.pontes/Desktop/PySparkETL/kaggle_files/labels_Covid.csv'
    df = spark.read.option("header", "true").csv(data_file_path)
    print("Covid labels DataFrame: ")
    df.show(5)

    return df
