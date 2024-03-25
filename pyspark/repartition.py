import pyspark
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("RepartitionExample") \
    .getOrCreate()

data = [("John", 25), ("Alice", 30), ("Bob", 35), ("Jane", 40)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Check the current number of partitions
print("Number of partitions before repartition: ", df.rdd.getNumPartitions())

# Repartition the DataFrame into 2 partitions
df_repartitioned = df.repartition(2)

# Check the number of partitions after repartitioning
print("Number of partitions after repartition: ", df_repartitioned.rdd.getNumPartitions())
