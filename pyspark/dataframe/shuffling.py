import pyspark
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("ShufflingExample") \
    .getOrCreate()

# Create a DataFrame with some sample data
data = [("John", 25), ("Alice", 30), ("Bob", 35), ("Jane", 40)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Repartition the DataFrame to introduce shuffling
df_repartitioned = df.repartition(2)

# Perform a groupBy operation which involves shuffling
grouped_df = df_repartitioned.groupBy("Age").count()

# Show the resulting DataFrame
grouped_df.show()
