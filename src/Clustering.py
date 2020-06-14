%pyspark
# load the data from S3 to Spark cluster
prq = "s3a://edgarlogsdata/log2017.parquet"
df = spark.read.parquet(prq)
df.count() # 4,588,193,236 rows

# aggregate the set of cik that an ip visited for each date
from pyspark.sql.functions import collect_set
df1 = df.groupby('date','ip').agg(collect_set('cik').alias("cik"))
N = df1.count() # 16,397,320 date-ip combinations

# use CountVectorizer to transfer the set of cik into matrix of counts per cik
from pyspark.ml.feature import CountVectorizer
cv = CountVectorizer(inputCol="cik", outputCol="features", minDF=m)
model = cv.fit(df1)
new_df = model.transform(df1)

# use KMeans to cluster the features column
from pyspark.ml.clustering import KMeans
kmeans = KMeans(k=K, seed=1)
model = kmeans.fit(new_df.select('features'))
result = model.transform(new_df)

# compute MSE = SSE/N
MSE = model.computeCost(new_df)/N

# write result to S3 for storage
output = "s3a://edgarlogoutput/result.csv"
result.repartition(1).select("date","prediction")\
    .write.csv(path=output, mode="append", header="true")

# use PCA to dimentionally reduce the features column for visualization
from pyspark.ml.feature import PCA
pca = PCA(k=2, inputCol="features", outputCol="pcaFeatures")
model = pca.fit(new_df)
result2 = model.transform(result).select("pcaFeatures", "prediction")
result2.show()
