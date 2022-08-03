from pyspark.sql import (SparkSession, functions as F)
from pyspark.sql.functions import regexp_extract
from pyspark.sql.types import StructType, StructField, IntegerType

from height_by_gender_and_country_df import height_by_gender_and_country

__all__ = ["height_transformed"]


def height_transformed(spark: SparkSession):
    """

    :type spark: object
    """
    df = height_by_gender_and_country(spark)
    df_using_functions = df.withColumnRenamed("Country Name", "country_name").drop("Male Height in Ft").withColumn(
        "random_column", F.when(F.col("Female Height in Cm") > 170.0, "tall").otherwise("short")).withColumn(
        "regex_extract", regexp_extract(F.col("country_name"), "Brazil|Iceland", 0))

    print("Height by gender and country transformed DataFrame: ")
    df_using_functions.show(5)

    data_array = [[1, 2], [2, 4], [3, 6], [4, 7]]
    schema = StructType([StructField('Rank', IntegerType(), True), StructField('extra_column', IntegerType(), True)])
    df_from_array = spark.createDataFrame(data_array, schema)

    join_condition = df_using_functions["Rank"] == df_from_array["Rank"]
    # Random join to simulate
    final_df = df_using_functions.join(df_from_array, join_condition, "left")

    return final_df
