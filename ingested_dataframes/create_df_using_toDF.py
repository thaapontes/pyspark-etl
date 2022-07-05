from pyspark.sql import SparkSession
from pyspark.sql.types import Row

"""
    Dataframe using toDF
"""

__all__ = ["create_dataframe_from_row"]


def create_dataframe_from_row(spark: SparkSession):
    data_row = [Row("chevrolet chevelle malibu", 18, "1970-01-01", "USA"),
                Row("chevrolet abc defg", 18, "1976-01-01", "BR")]

    df_from_row = data_row.toDF("name", "number", "date", "country")

    print("From using toDF: ")
    df_from_row.show(5)
    return df_from_row
