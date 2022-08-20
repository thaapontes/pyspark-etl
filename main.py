import logging
from json import dumps, loads

from pyspark.sql import SparkSession, DataFrame

import ingested_dataframes
import transformed_dataframes
from udfs import register_udf


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
        parsed_schema = loads(schema_as_json)
        logging.warning("*** Schema as JSON: {}".format(dumps(parsed_schema, indent=2)))


def get_query_plan(df: DataFrame):
    df.explain()


def query_on_df(df: DataFrame, column: str):
    df.createOrReplaceTempView("dfs")
    my_query = spark.sql('SELECT ' + column + ' FROM dfs LIMIT 3')
    my_query.show()


def load_df(df: DataFrame, df_name: str):
    df.coalesce(1).write.format('json').save(df_name + '.json')


def persist_mode(mode: str, df: DataFrame):
    """
    :param df: the dataframe will wish to persist
    :param mode: cache can be used for datasets that will be repeatedly computed and are not too large checkpoint can
    be used for datasets that takes a long time to run, or when the computing chain is too long or depends on too
    many datasets
    cache save in memory/disk and remember lineage whilst checkpoint saves to an HDFS and forgets the lineage
    resource: https://mallikarjuna_g.gitbooks.io/sparkinternals/content/cache-and-checkpoint.html
    """
    if mode == 'cache':
        df.cache()
    elif mode == 'checkpoint':
        df.checkpoint()


if __name__ == '__main__':
    # Create a session on a local master
    spark = SparkSession.builder \
        .appName("reading csv") \
        .master("local") \
        .getOrCreate()

    '''
    Call udfs
    '''
    register_udf(spark)

    ''' 
    Call ingested dataframes Multiple ways to ingest different formats:
    https://github.com/jgperrin/net.jgp.books.spark.ch07/tree/master/src/main/python
    Reading a DF:
    - format: json, csv, text, parquet (default format in Spark, not needed to put .format)
    - enforce schema or inferSchema = true
    - path in load() or options
    - zero or more options like mode (failFast, dropMalformed, permissive), 
    dateFormat, header, sep, nullValue
    '''
    for dataframe in ingested_dataframes.__all__(spark):
        dataframe
    # logging.warning("*** Right after ingestion")
    # get_number_of_records(eval(i)(spark))
    # get_number_of_partitions(eval(i)(spark))
    # get_number_of_partitions_after_repartition(eval(i)(spark), 4)

    ''' 
    Writing a DF:
    - format: json, csv, text, parquet (default format in Spark, not needed to put .format)
    - mode: overwrite, append, ignore, errorIfExists
    - path in save()
    - zero or more options
    '''

    for dataframe in transformed_dataframes.__all__(spark):
        dataframe
        # logging.warning("*** Right after transformations")
        # get_schema(eval(i)(spark), 'json')
        # get_query_plan(eval(i)(spark))
        # query_on_df(eval(i)(spark), 'price_range')
        # load_df(eval(i)(spark), i)

    # Create RDD from a list
    rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
    rddCollect = rdd.collect()
    # print("Number of Partitions: " + str(rdd.getNumPartitions()))
    # print("Action: First element: " + str(rdd.first()))
    # print(rddCollect)

    # Create empty RDD
    emptyRDD = spark.sparkContext.emptyRDD()
    emptyRDD2 = spark.sparkContext.parallelize([])
    # print("is Empty RDD : " + str(emptyRDD2.isEmpty()))

    # Stop SparkSession at the end of the application
    spark.stop()

    '''
    Read lines from file stream
    reference: Spark in action ch10
    df = spark.readStream.format("text").load("/tmp/")

    query = df.writeStream.outputMode("append")\
        .format("console")\
        .option("truncate", False)\
        .option("numRows", 3)\
        .start()

    query.awaitTermination()
    '''

    # chapter 08: database
    # chapter 16

    '''
    Next step: save file to database
    '''
    # Refers to https://github.com/jgperrin/net.jgp.books.spark.ch08/tree/master/src/main/python
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

    # THIRD WAY
    '''
    Reading from a remote DB
    
    driver = "org.postgresql.Driver"
    url = "jdbc:postgresql://localhost:5432/rtjvm"
    user = "docker"
    password = "docker"

    def read_table(table_name: str) = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", s"public.$table_name")
    .load()
    '''
    # Tables in spark are transient, remember to write tables before finishing the session with spark.stop()
