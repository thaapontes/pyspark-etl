from pyspark.sql import SparkSession

__all__ = ["rdd"]


def rdd(spark: SparkSession):
    sc = spark.sparkContext

    numbers = range(1, 1000)
    numbers_rdd = sc.parallelize(numbers)
    rdd_collect = numbers_rdd.collect()

    return rdd_collect
