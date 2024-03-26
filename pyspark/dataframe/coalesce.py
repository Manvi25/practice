import pyspark
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("CoalesceExample") \
    .getOrCreate()

# Create a DataFrame
data = [("John", 25), ("Alice", 30), ("Bob", 35), ("Jane", 40)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Repartition the DataFrame into 4 partitions
df_repartitioned = df.repartition(4)

# Check the current number of partitions
print("Number of partitions before coalesce: ", df_repartitioned.rdd.getNumPartitions())

# Coalesce the DataFrame into 2 partitions
df_coalesced = df_repartitioned.coalesce(2)

# Check the number of partitions after coalesce
print("Number of partitions after coalesce: ", df_coalesced.rdd.getNumPartitions())
