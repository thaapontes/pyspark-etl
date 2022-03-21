import os

from pyspark import SparkContext

"""
    Csv example from: https://www.kaggle.com/datasets
    Huge file: 16GB
    Diff between coalesce and repartition: https://towardsdatascience.com/how-to-efficiently-re-partition-spark-dataframes-c036e8261418
"""
def ge_cars(sc: SparkContext):
    current_dir = "/Users/thabata.pontes/Desktop/PySparkETL/kaggle_csvs/"
    relative_path = "MyAuto_ge_Cars_Data.csv"
    absolute_file_path = os.path.join(current_dir, relative_path)
    df = sc.read.option("header", "true").csv(absolute_file_path)

    print("partitions count before repartition:" + str(df.rdd.getNumPartitions()))
    print("partitions size before repartition:" + str(df.rdd.mapPartitionsWithIndex(lambda x, it: [(x, sum(1 for _ in it))]).collect()))

    df_repartitioned = df.repartition(95)
    df_repartitioned.show(5)
    print("partitions count after repartition:" + str(df_repartitioned.rdd.getNumPartitions()))
    print("partitions size after repartition:" + str(
        df_repartitioned.rdd.mapPartitionsWithIndex(lambda x, it: [(x, sum(1 for _ in it))]).collect()))


def __all__():
    return 'GeCars'