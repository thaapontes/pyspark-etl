from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

# https://towardsdatascience.com/create-your-first-etl-pipeline-in-apache-spark-and-python-ec3d12e2c169
def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.


if __name__ == '__main__':
    spark = SparkSession.builder.appName("reading csv").getOrCreate()

    data_file = '/Users/thabata.pontes/Downloads/life_insurance.csv'
    sdfData = spark.read.csv(data_file).cache()
    print('Total Records = {}'.format(sdfData.count()))
    sdfData.show()

    # set variable to be used to connect the database
    database = "database-1.cg9pkmbnsltc.sa-east-1.rds.amazonaws.com"
    table = sdfData
    user = "thaapontes"
    password = "t28230729"

    # read table data into a spark dataframe
    jdbcDF = spark.read.format("jdbc") \
        .option("url", f"{database}") \
        .option("dbtable", table) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .load()

    # show the data loaded into dataframe
    jdbcDF.show()
