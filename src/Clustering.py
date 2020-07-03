# To be run with pyspark
from pyspark.sql import SparkSession
import sys
from pyspark.sql.functions import collect_set
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import PCA
from pyspark.sql.types import StringType


'''
This batch job is to cluster logs that exhibit similar patterns.

Input file column names:
    'ip', 'date', 'time', 'zone', 'cik', 'accession', 'extention', 'code',
    'size','idx', 'norefer', 'noagent', 'find', 'crawler', 'browser'
'''


if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Usage: input_file output_file minDF K")
        sys.exit(-1)

    # command line arguments
    input_file = sys.argv[1]  # "s3a://edgarlogsdata/log2017.parquet"
    output_file = sys.argv[2] # "s3a://edgarlogoutput/result.csv"
    minDF = int(sys.argv[3])  # minimum document frequency
    K = int(sys.argv[4])      # number of clusters
    P = int(sys.argv[5])      # first P principal components

    # create SparkSession
    spark = SparkSession\
            .builder\
            .appName("LogClustering")\
            .getOrCreate()

    # load the data from S3 to Spark cluster
    df = spark.read.parquet(input_file)
    df.count()
    # 4,588,193,236 rows

    # aggregate the set of cik that an ip visited for each date
    df1 = df.groupby('date','ip').agg(collect_set('cik').alias("cik_set"))
    N = df1.count()
    # 16,397,320 date-ip combinations

    # use CountVectorizer to transfer the set of cik into matrix of counts per cik
    cv = CountVectorizer(inputCol="cik_set", outputCol="features", minDF=minDF)
    model = cv.fit(df1)
    new_df = model.transform(df1)
    new_df.show()
    # 262,144 distinct documents (cik) viewed by users (ip)

    # use KMeans to cluster the features column
    kmeans = KMeans(k=K, seed=1)
    model = kmeans.fit(new_df.select('features'))
    result = model.transform(new_df)

    # compute MSE = SSE/N
    MSE = model.computeCost(new_df)/N

    # view the cluster occupencies to ensure no outlier
    # i.e. no one data point is getting its own cluster
    result.groupby("prediction").count().show()

    # convert array<string> data type to string type for csv
    result2 = result.withColumn("cik-set", result["cik_set"].cast(StringType()))

    # write result to S3 for storage
    result2.repartition(1)\
           .select("date","ip","cik-set","prediction")\
           .write\
           .csv(path=output_file, mode="append", header="true")

    # use PCA to dimensionally reduce the features column
    pca = PCA(k=P, inputCol="features", outputCol="pcaFeatures")
    model = pca.fit(result)
    result3 = model.transform(result).select("pcaFeatures", "prediction")
    
    # use t-SNE for visualization

    spark.stop()
