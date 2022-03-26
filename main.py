import logging

from pyspark.sql import SparkSession
from dataframes import ge_cars, height_by_gender_and_country, labels_covid, create_dataframe_from_array
# TODO find a way to remove these imports

import dataframes

if __name__ == '__main__':
    # Create a session on a local master
    spark = SparkSession.builder \
        .appName("reading csv") \
        .master("local") \
        .getOrCreate()

    # Call dataframes
    for i in dataframes.__all__:
        eval(i)(spark)

    # How many records in the df and its content
    logging.warning("*** Right after ingestion")
    # print('Total Records = {}'.format(insuranceDataFrame.count()))
    # insuranceDataFrame.show(5)
    # pjDataFrame.show(5)

    # Transforming the data
    # pjTransformed = pjDataFrame \
    #     .withColumn("is_aware", when(col("current_stage") == "aware", 1).otherwise(0)) \
    #     .groupBy('is_aware').count()

    logging.warning("*** Right after transformation")
    # pjTransformed.show(5)

    # How many partitions the dataframe has (but for this to happen, it must be a rdd)
    # partitions = pjTransformed.rdd.getNumPartitions()
    # print("partitions count:" + str(partitions))

    # repartitioned = pjTransformed.repartition(4).rdd.getNumPartitions()
    # print("partitions count after repartition:" + str(repartitioned))

    # Combining both DataFrames
    # allDfs = insuranceDataFrame.join(pjDataFrame, 'customer__id')
    # allDfs.show()

    # Get schema
    logging.warning("*** Schema as a tree:")
    # allDfs.printSchema()

    # logging.warning("*** Schema as string: {}".format(allDfs.schema))
    # schemaAsJson = allDfs.schema.json()
    # parsedSchemaAsJson = json.loads(schemaAsJson)

    # logging.warning("*** Schema as JSON: {}".format(json.dumps(parsedSchemaAsJson, indent=2)))

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
