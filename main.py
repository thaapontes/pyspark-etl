import json
import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, lit
from pyspark.sql.types import StructType, StructField, IntegerType


def print_hi(name):
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.


if __name__ == '__main__':
    # Create a session on a local master
    spark = SparkSession.builder \
        .appName("reading csv") \
        .master("local") \
        .getOrCreate()

    # Read CSV file - 1
    mode = ""
    data_file = '/Users/thabata.pontes/Downloads/life_insurance.csv'
    insuranceDataFrame = spark.read.option("header", "true").csv(data_file).cache()
    '''
    SaveMode.Overwrite: overwrite the existing data.
    SaveMode.Append: append the data.
    SaveMode.Ignore: ignore the operation (i.e. no-op).
    SaveMode.ErrorIfExists: throw an exception at runtime.
    '''
    if mode.lower != "noop":
        insuranceDataFrame = insuranceDataFrame.withColumn("avg", lit(2))
    if mode.lower == "full":
        insuranceDataFrame = insuranceDataFrame.drop("date")
        print("full")

    # Read CSV file - 2
    pjDataFrame = spark.read \
        .format("csv") \
        .option("header", "true") \
        .load("/Users/thabata.pontes/Downloads/pj.csv")

    # Read CSV file - 3
    current_dir = "/Users/thabata.pontes/Downloads/"
    relative_path = "pj.csv"
    absolute_file_path = os.path.join(current_dir, relative_path)

    df = spark.read.csv(header=True, inferSchema=True, path=absolute_file_path)

    # Create a dataframe from array
    data_array = [[1], [2], [3], [4]]
    schema = StructType([StructField('age', IntegerType(), True)])
    df_from_array = spark.createDataFrame(data_array, schema)

    # How many records in the df and its content
    logging.warning("*** Right after ingestion")
    print('Total Records = {}'.format(insuranceDataFrame.count()))
    insuranceDataFrame.show(5)
    pjDataFrame.show(5)

    # Transforming the data
    pjTransformed = pjDataFrame \
        .withColumn("is_aware", when(col("current_stage") == "aware", 1).otherwise(0)) \
        .groupBy('is_aware').count()

    logging.warning("*** Right after transformation")
    pjTransformed.show(5)

    # How many partitions the dataframe has (but for this to happen, it must be a rdd)
    partitions = pjTransformed.rdd.getNumPartitions()
    print("partitions count:" + str(partitions))

    repartitioned = pjTransformed.repartition(4).rdd.getNumPartitions()
    print("partitions count after repartition:" + str(repartitioned))

    # Combining both DataFrames
    allDfs = insuranceDataFrame.join(pjDataFrame, 'customer__id')
    allDfs.show()

    # Get schema
    logging.warning("*** Schema as a tree:")
    allDfs.printSchema()

    logging.warning("*** Schema as string: {}".format(allDfs.schema))
    schemaAsJson = allDfs.schema.json()
    parsedSchemaAsJson = json.loads(schemaAsJson)

    logging.warning("*** Schema as JSON: {}".format(json.dumps(parsedSchemaAsJson, indent=2)))

    # Access to catalyst query plan
    allDfs.explain()

    # Querying on it
    allDfs.createOrReplaceTempView("dfs")
    myQuery = spark.sql('SELECT customer__id FROM dfs LIMIT 3')
    myQuery.show()

    # Load
    # pjTransformed.coalesce(1).write.format('json').save('pj.json')

    # Stop SparkSession at the end of the application
    spark.stop()

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
