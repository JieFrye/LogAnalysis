%pyspark
# load csv files (11 min 57 sec.)
file = "s3a://edgarlogsdata/log2017*.csv"
data = spark.read.csv(file, header=True)
data.count()

# write csv file to parquet format
data.write.parquet("s3a://edgarlogsdata/log2017.parquet")

# load parquet files (1 min 34 sec.)
prq = "s3a://edgarlogsdata/log2017.parquet"
df = spark.read.parquet(prq)
df.count()
df.show()
