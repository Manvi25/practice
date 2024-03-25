import pyspark
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("CachePersistExample") \
    .getOrCreate()

# Create a DataFrame
data = [("John", 25), ("Alice", 30), ("Bob", 35), ("Jane", 40)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Cache the DataFrame
df.cache()

# Action to trigger caching
df.count()

# Check if DataFrame is cached
print("Is DataFrame cached:", df.is_cached)

# Persist the DataFrame with storage level MEMORY_AND_DISK
df.persist(pyspark.StorageLevel.MEMORY_AND_DISK)

# Action to trigger persisting
df.show()

# Check if DataFrame is persisted
print("Is DataFrame persisted:", df.is_cached)

# Unpersist the DataFrame
df.unpersist()

# Check if DataFrame is still persisted after unpersisting
print("Is DataFrame still persisted:", df.is_cached)
