from pyspark.sql import SparkSession

from cars_df import *
from covid_df import *

def __all__(spark: SparkSession):
    list_all_functions = (ge_cars(spark),
                          labels_covid(spark)
                          )

    return list_all_functions