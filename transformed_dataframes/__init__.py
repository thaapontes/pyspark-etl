from pyspark.sql import SparkSession

from cars_grouped import *
from height_grouped import *
from movies_dates import *
from smartphones_ways_of_selecting import *
from spotify_grouped import *
from smartphones_aggregated import *

def __all__(spark: SparkSession):
    list_all_functions = (cars_price(spark),
                          height_transformed(spark),
                          movies_dates(spark),
                          smartphones_select(spark),
                          artists_danceability(spark),
                          smartphones_agg(spark))

    return list_all_functions