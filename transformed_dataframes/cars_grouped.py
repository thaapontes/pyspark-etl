from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

from ingested_dataframes import ge_cars

__all__ = ["cars_price"]


def cars_price(sc: SparkSession):
    df = ge_cars(sc)
    df_agg = df.withColumn("price_range", when(col("Price ($)") < 10000, "low").when((col("Price ($)") > 10000) & (col("Price ($)") < 30000),
                                                                                     "medium").otherwise(
        "high")).groupBy("Manufacturer", "price_range").count()
    df_agg.show(5)
    return df_agg
