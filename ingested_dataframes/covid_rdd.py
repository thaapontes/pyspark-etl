from pyspark.sql import SparkSession
from pyspark.sql.functions import col

"""
    Csv example from: https://www.kaggle.com/datasets
    Medium file: 1GB
"""

__all__ = ["labels_covid_rdd"]


def labels_covid_rdd(spark: SparkSession):
    data_file_path = '/Users/thabata.pontes/Desktop/PySparkETL/kaggle_files/labels_Covid.csv'
    df = spark.read.option("header", "true").csv(data_file_path)
    rdd = df.select(col("ClassId").alias("class_id"), col("Name").alias("name")).rdd

    rdd.toDF().show()
    return rdd
