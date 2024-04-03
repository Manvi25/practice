from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Explode Example") \
    .getOrCreate()

# Sample DataFrame
data = [(1, "John", ["Python", "Java", "SQL"]),
        (2, "Alice", ["JavaScript", "HTML", "CSS"]),
        (3, "Manvi",None)]
columns = ["id", "name", "skills"]
df = spark.createDataFrame(data, columns)

# Show the DataFrame
df.show()

# Explode function
exploded_df = df.select("id", "name", explode("skills").alias("skill"))
exploded_df.show()
