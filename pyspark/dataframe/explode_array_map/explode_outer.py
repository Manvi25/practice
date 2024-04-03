from pyspark.sql import SparkSession
from pyspark.sql.functions import explode_outer
spark = SparkSession.builder \
    .appName("Explode Outer DataFrame Example") \
    .getOrCreate()
data = [(1, "John", ["Python", "Java", "SQL"]),
        (2, "Alice", None)]
columns = ["id", "name", "skills"]
df = spark.createDataFrame(data, columns)
df.show()

# Explode the 'skills' array column using explode_outer
exploded_df = df.select("id", "name", explode_outer("skills").alias("skill"))
exploded_df.show()
