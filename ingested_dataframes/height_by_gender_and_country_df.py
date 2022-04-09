from pyspark.sql import SparkSession

"""
    Csv example from: https://www.kaggle.com/datasets
    Small file: 4kB
"""

__all__ = ["height_by_gender_and_country"]


def height_by_gender_and_country(sc: SparkSession):
    data_file_path = '/Users/thabata.pontes/Desktop/PySparkETL/kaggle_csvs/Height_of_Male_and_Female_by_Country_2022.csv'
    df = sc.read.option("header", "true").csv(data_file_path).cache()
    df.show(5)

    return df