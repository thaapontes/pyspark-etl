from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col, countDistinct, avg

from smartphones_df import smartphones_dataframe

__all__ = ["smartphones_agg"]


def smartphones_agg(spark: SparkSession):
    df = smartphones_dataframe(spark)

    # Counting all
    df_count_all_rows = df.select(count("*"))
    df_count_all_rows.show()
    # Counting all except null values
    df_count_rows_except_nulls = df.select(count(col("make")))
    df_count_rows_except_nulls.show()
    # Counting distinct
    df_count_distinct_rows = df.select(countDistinct(col("make")))
    df_count_distinct_rows.show()

    # Min, max, avg
    df_min_row = df.select(min(col("make")))
    df_min_row.show()
    # Grouping
    df_count_by_make = df.groupBy("make").count()
    df_count_by_make.show()
    df_avg_model_by_make = df.groupBy("make").avg("model")
    df_avg_model_by_make.show()
    df_agg_by_make = df.groupBy("make").agg(avg("model").alias("avg_model"), count("*").alias("count"))
    df_agg_by_make.show()

    return df_count_all_rows
