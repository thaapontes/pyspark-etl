from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

# https://towardsdatascience.com/create-your-first-etl-pipeline-in-apache-spark-and-python-ec3d12e2c169
def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.


if __name__ == '__main__':
    scSpark = SparkSession.builder.appName("reading csv").getOrCreate()

    data_file = '/Users/thabata.pontes/Downloads/life_insurance.csv'
    sdfData = scSpark.read.csv(data_file).cache()
    print('Total Records = {}'.format(sdfData.count()))
    sdfData.show()
