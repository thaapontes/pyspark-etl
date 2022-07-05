from pyspark.sql import SparkSession
from pyspark.sql.types import Row

"""
    Dataframe from Row
"""

__all__ = ["create_dataframe_from_row"]


def create_dataframe_from_row(spark: SparkSession):
    data_row = [Row("chevrolet chevelle malibu", 18, 8, 307, 130, 3504, 12.0, "1970-01-01", "USA"),
                Row("chevrolet abc defg", 18, 8, 307, 130, 1234, 15.7, "1976-01-01", "BR")]
    df_from_row = spark.createDataFrame(data_row)

    print("From Row DataFrame: ")
    df_from_row.show(5)
    return df_from_row
