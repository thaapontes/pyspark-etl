import os

from pyspark import SparkContext

"""
    Csv example from: https://www.kaggle.com/datasets
    Huge file: 16GB
"""
def ge_cars(sc: SparkContext):
    current_dir = "/Users/thabata.pontes/Desktop/PySparkETL/kaggle_csvs/"
    relative_path = "MyAuto_ge_Cars_Data.csv"
    absolute_file_path = os.path.join(current_dir, relative_path)
    df = sc.read.option("header", "true").csv(absolute_file_path)
    print("partitions count before repartition:" + str(partitions))
    df.rdd.get.getNumPartitions()
    df_repartitioned = df.repartition(95)
    df_repartitioned.show(5)
