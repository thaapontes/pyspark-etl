from pyspark.sql import SparkSession

"""
    Csv example from: https://www.kaggle.com/datasets
    Medium file: 1GB
"""

__all__ = ["labels_covid"]


def labels_covid(sc: SparkSession):
    data_file_path = '/Users/thabata.pontes/Desktop/PySparkETL/kaggle_csvs/labels_Covid.csv'
    df = sc.read.option("header", "true").csv(data_file_path)
    df.show(5)
