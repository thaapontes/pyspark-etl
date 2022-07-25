from pyspark.sql import SparkSession
from pyspark.sql.functions import col, column, expr

from smartphones_df import smartphones_dataframe

__all__ = ["smartphones_select"]


def smartphones_select(spark: SparkSession):
    df = smartphones_dataframe(spark)

    '''
    it's not possible to use $ in pyspark,
    since it uses implicit type def in scala that does not exist in python 
    '''
    df_selected = df.select(col("make"), column("model"), expr("platform"), df.col("camera_megapixels"))
    df_selected_expr = df_selected.selectExpr("make", "model", "camera_megapixels + 1")

    return df_selected_expr
