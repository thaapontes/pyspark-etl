import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import when, col
import os


def print_hi(name):
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.


if __name__ == '__main__':
    # Create a session on a local master
    spark = SparkSession.builder \
        .appName("reading csv") \
        .master("local") \
        .getOrCreate()

    # Read CSV file - 1
    data_file = '/Users/thabata.pontes/Downloads/life_insurance.csv'
    insuranceDataFrame = spark.read.option("header", "true").csv(data_file).cache()

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

    # How many records in the df and its content
    print('Total Records = {}'.format(insuranceDataFrame.count()))
    insuranceDataFrame.show(5)
    pjDataFrame.show(5)

    # Transforming the data
    pjTransformed = pjDataFrame \
        .withColumn("is_aware", when(col("current_stage") == "aware", 1).otherwise(0)) \
        .groupBy('is_aware').count()

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
    allDfs.printSchema()

    # Access to catalyst query plan
    allDfs.explain()

    # Querying on it
    allDfs.createOrReplaceTempView("dfs")
    myQuery = spark.sql('SELECT customer__id FROM dfs LIMIT 3')
    myQuery.show()

    # Load
    pjTransformed.coalesce(1).write.format('json').save('pj.json')

    #Stop SparkSession at the end of the application
    spark.stop()

    '''
    Next step: save file to database
    '''
    # TODO get credentials from aws RDS

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
