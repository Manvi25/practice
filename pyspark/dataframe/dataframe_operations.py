from pyspark.sql import SparkSession
# Initialize Spark session
spark = SparkSession.builder \
    .appName("DataFrame Operations") \
    .getOrCreate()
# Define data
data = [("John", "Doe", 30, 101),
        ("Jane", "Smith", 25, 102),
        ("Bob", "Johnson", 35, 103)]
# Create DataFrame
columns = ["firstname", "lastname", "age", "rollnumber"]
df = spark.createDataFrame(data, schema=columns)

# Show DataFrame
print("Original DataFrame:")
df.show()

# Print schema
print("Schema of the DataFrame:")
df.printSchema()

# Select columns
print("Selecting columns:")
df.select("firstname", "age").show()

# Filtering data
print("Filtering data:")
df_filtered = df.filter("age > 25")
df_filtered.show()
# Add new column
print("Adding a new column:")
df_with_fullname = df.withColumn("fullname", df.firstname + " " + df.lastname)
df_with_fullname.show()
# Sorting
print("Sorting by age in descending order:")
df_sorted = df.orderBy(df.age.desc())
df_sorted.show()
# Counting rows
print("Counting the number of rows:")
print("Total rows:", df.count())
# Renaming a column
print("Renaming the 'age' column to 'years_old':")
df_renamed = df.withColumnRenamed("age", "years_old")
df_renamed.show()