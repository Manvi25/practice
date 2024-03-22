from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("RDD to DataFrame") \
    .getOrCreate()

# Sample data as an RDD
rdd = spark.sparkContext.parallelize([(1, 'Alice', 30), (2, 'Bob', 35), (3, 'Charlie', 40)])

# Convert RDD to DataFrame
df = spark.createDataFrame(rdd, schema=["id", "name", "age"])

# Show DataFrame
df.show()
