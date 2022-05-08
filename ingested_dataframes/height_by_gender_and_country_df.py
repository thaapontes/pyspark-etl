from pyspark.sql import SparkSession

"""
    Csv example from: https://www.kaggle.com/datasets
    Small file: 4kB
"""

__all__ = ["height_by_gender_and_country"]

# INGESTION WITHOUT SCHEMA
def height_by_gender_and_country(spark: SparkSession):
    data_file_path = '/Users/thabata.pontes/Desktop/PySparkETL/kaggle_files/Height_of_Male_and_Female_by_Country_2022.csv'
    df = spark.read.option("header", "true").csv(data_file_path).cache()
    print("Height by gender and country DataFrame: ")
    df.show(5)

    return df
