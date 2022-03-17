from pyspark import SparkContext

sc = SparkContext() #allow to interact with DataSet and DataFrame
n = sc.parallelize([4, 10, 9, 7])
print(n.take(3))
