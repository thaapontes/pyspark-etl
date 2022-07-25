import os

from pyspark.sql import SparkSession

__all__ = ["enforcing_cars_schema"]

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, DateType


# INGESTION ENFORCING SCHEMA
def enforcing_cars_schema(spark: SparkSession):
    current_dir = "/Users/thabata.pontes/Desktop/PySparkETL/kaggle_files/"
    relative_path = "cars.json"
    absolute_file_path = os.path.join(current_dir, relative_path)

    cars_schema = StructType(
        [StructField("Name", StringType()),
         StructField("Miles_per_Gallon", DoubleType()),
         StructField("Cylinders", LongType()),
         StructField("Displacement", DoubleType()),
         StructField("Horsepower", LongType()),
         StructField("Weight_in_lbs", LongType()),
         StructField("Acceleration", DoubleType()),
         StructField("Year", DateType()),
         StructField("Origin", StringType())]
    )

    '''
        MODE
        failFast: throw exception when it found malformed records when parsing
        dropMalformed: drop malformed records when parsing
        permissive (default)
        '''

    df = spark.read.format("json") \
        .schema(cars_schema) \
        .option("dateFormat", "YYYY-MM-dd") \
        .option("mode", "failFast") \
        .load(absolute_file_path)

    print("Cars DataFrame: ")
    df.show(5)

    return df
