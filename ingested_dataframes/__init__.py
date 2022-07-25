from pyspark.sql import SparkSession

from cars_df import *
from covid_df import *
from create_df_from_row import *
from enforcing_schema import *
from height_by_gender_and_country_df import *
from random_df_from_array import *
from smartphones_df import *
from spotify_2018_top_songs import *
from thailand_public_train import *

def __all__(spark: SparkSession):
    list_all_functions = (ge_cars(spark),
                          labels_covid(spark),
                          create_dataframe_from_row(spark),
                          enforcing_cars_schema(spark),
                          height_by_gender_and_country(spark),
                          create_dataframe_from_array(spark),
                          smartphones_dataframe(spark),
                          spotify_songs(spark),
                          thailand_public(spark))

    return list_all_functions