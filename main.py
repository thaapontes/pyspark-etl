import logging

from pandas._libs import json
from pyspark.pandas import DataFrame
from pyspark.sql import SparkSession
from ingested_dataframes import ge_cars, height_by_gender_and_country, labels_covid, create_dataframe_from_array
from transformed_dataframes import cars_price
# TODO find a way to remove these imports

import ingested_dataframes
import transformed_dataframes


def get_number_of_records(df: DataFrame):
    print('Total Records = {}'.format(df.count()))


def get_number_of_partitions(df: DataFrame):
    partitions = df.rdd.getNumPartitions()
    print("partitions count:" + str(partitions))


def get_number_of_partitions_after_repartition(df: DataFrame, number_of_partitions: int):
    repartitioned = df.repartition(number_of_partitions).rdd.getNumPartitions()
    print("partitions count after repartition:" + str(repartitioned))


def get_schema(df: DataFrame, schema_type: str):
    if schema_type == 'tree':
        logging.warning("*** Schema as a tree:")
        df.printSchema()
    elif schema_type == 'string':
        logging.warning("*** Schema as string: {}".format(df.schema))
    else:
        schema_as_json = df.schema.json()
        parsed_schema = json.loads(schema_as_json)
        logging.warning("*** Schema as JSON: {}".format(json.dumps(parsed_schema, indent=2)))


if __name__ == '__main__':
    # Create a session on a local master
    spark = SparkSession.builder \
        .appName("reading csv") \
        .master("local") \
        .getOrCreate()

    # Call ingested dataframes
    for i in ingested_dataframes.__all__:
        eval(i)(spark)
        logging.warning("*** Right after ingestion")
        # get_number_of_records(eval(i)(spark))
        # get_number_of_partitions(eval(i)(spark))
        # get_number_of_partitions_after_repartition(eval(i)(spark), 4)

    for i in transformed_dataframes.__all__:
        eval(i)(spark)
        logging.warning("*** Right after transformations")
        get_schema(eval(i)(spark), 'json')

    # Access to catalyst query plan
    # allDfs.explain()

    # Querying on it
    # allDfs.createOrReplaceTempView("dfs")
    myQuery = spark.sql('SELECT customer__id FROM dfs LIMIT 3')
    myQuery.show()

    # Load
    # pjTransformed.coalesce(1).write.format('json').save('pj.json')

    # Stop SparkSession at the end of the application
    spark.stop()

    # chapter 05

    '''
    Next step: save file to database
    '''
    # TODO get credentials from aws RDS

    # FIRST WAY
    # set variable to be used to connect the database
    # database = "database-1.cg9pkmbnsltc.sa-east-1.rds.amazonaws.com"
    # table = sdfData
    # user = "thaapontes"
    # password = "t28230729"

    # read table data into a spark dataframe
    # jdbcDF = spark.read.format("jdbc") \
    #     .option("url", f"{database}") \
    #     .option("dbtable", table) \
    #     .option("user", user) \
    #     .option("password", password) \
    #     .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    #     .load()

    # show the data loaded into dataframe
    # jdbcDF.show()

    # SECOND WAY - spark in action chapter 02
    # dbConnectionUrl = "jdbc:postgresql://localhost/spark_labs"
    # properties = = {"driver":"org.postgresql.Driver", "user":"jgp", "password":"Spark<3Java"}
    # df.write.jdbc(mode='overwrite', url=dbConnectionUrl, table="ch02", properties=prop)

    # Tables in spark are transient, remember to write tables before finishing the session with spark.stop()
