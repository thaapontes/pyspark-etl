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
