import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

"""
    Csv example from: https://www.kaggle.com/datasets
    Medium file: 288MB
"""

__all__ = ["enem_test_and_train"]


# PARQUET INGESTION WITHOUT SCHEMA
def enem_test_and_train(spark: SparkSession):
    current_dir = "/Users/thabata.pontes/Desktop/PySparkETL/kaggle_files/"
    relative_path_train = "train.parquet"
    relative_path_test = "test.parquet"
    absolute_file_path_train = os.path.join(current_dir, relative_path_train)
    absolute_file_path_test = os.path.join(current_dir, relative_path_test)

    df_train = spark.read.format("parquet").load(absolute_file_path_train)
    df_test = spark.read.format("parquet").load(absolute_file_path_test)

    extra_columns_list = ["NU_NOTA_CN", "NU_NOTA_CH", "NU_NOTA_LC", "NU_NOTA_MT", "NU_NOTA_REDACAO"]
    select_statement = []
    for col in extra_columns_list:
        select_statement.append(lit(None).alias(col))
    df_test = df_test.select(*df_test.columns, *select_statement)
    df = df_train.unionByName(df_test)

    print("Enem DataFrame: ")
    df.show(5)

    return df