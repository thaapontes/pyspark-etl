from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, current_date, current_timestamp, datediff, struct, split, array_contains

from movies_df import movies

__all__ = ["movies_dates"]


def movies_dates(spark: SparkSession):
    df = movies(spark)
    df_dates = df.withColumn("release_date", to_date(col("Release_Date"), "d-MMM-yy")) \
        .withColumn("today", current_date()) \
        .withColumn("right_now", current_timestamp()) \
        .withColumn("movie_age", datediff(col("Today"), col("release_date")) / 365) \
        .withColumn("profit_structure", struct(col("US_Gross"), col("Worldwide_Gross"))) \
        .withColumn("us_profit", col("profit_structure").getField("US_Gross")) \
        .withColumn("title_words", split(col("Title"), " |,")) \
        .withColumn("love_in_title", array_contains(col("title_words"), "Love"))

    '''
    How to deal with multiple date formats?
    Parse the df multiple times and union the small dfs afterwards
    '''
    print("Movies Df transformed")
    df_dates.show(5)

    return df_dates
