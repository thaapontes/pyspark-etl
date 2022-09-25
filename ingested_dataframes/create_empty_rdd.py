from pyspark.sql import SparkSession

__all__ = ["empty_rdd"]


def empty_rdd(spark: SparkSession):
    sc = spark.sparkContext

    empty_rdd_1 = sc.emptyRDD()
    empty_rdd_2 = sc.parallelize([])

    return empty_rdd_1
