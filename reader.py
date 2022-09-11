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
