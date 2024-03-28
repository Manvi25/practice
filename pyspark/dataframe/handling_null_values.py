from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a SparkSession
spark = SparkSession.builder \
    .appName("HandleNullValues") \
    .getOrCreate()

# Sample DataFrame with NULL values
data = [
    (1, "Manvi", None),
    (2, "Sakshi", 23),
    (3, None, 25),
    (4, "Priya", None),
    (5, "Ishita", 35)
]
columns = ["id", "name", "age"]
df = spark.createDataFrame(data, columns)

print("Original DataFrame:")
df.show()

# Filtering rows with NULL values in any column
filtered_df = df.filter(col("id").isNotNull() & col("name").isNotNull() & col("age").isNotNull())
print("\nFiltered DataFrame (Nulls Removed):")
filtered_df.show()


# Filling NULL values with a default value or string
filled_df = df.fillna({"age": 0, "name": "Unknown"})
print("\nDataFrame with NULLs Replaced (Filled with Default Values):")
filled_df.show()

# Choosing non-null values from multiple columns
coalesced_df = df.withColumn("selected_name", col("name").coalesce(col("age"), col("id")))
print("\nDataFrame with Coalesced Column (Choosing Non-Null Values):")
coalesced_df.show()

# Dropping rows with NULL values in any column
dropped_df = df.na.drop()
print("\nDataFrame with Rows Dropped (Nulls Removed):")
dropped_df.show()

# Filling NULL values with specific values
filled_df = df.fillna({"age": 0, "name": "Unknown"})
print("\nDataFrame with NULLs Replaced (Filled with Default Values):")
filled_df.show()

