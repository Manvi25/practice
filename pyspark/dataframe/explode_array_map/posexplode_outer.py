from pyspark.sql import SparkSession
from pyspark.sql.functions import posexplode_outer

spark = SparkSession.builder \
    .appName("PosExplode Outer Example") \
    .getOrCreate()
data = [(1, "John", ["Python", "Java", "SQL"]),
        (2, "Alice", None)]
columns = ["id", "name", "skills"]
df = spark.createDataFrame(data, columns)
df.show()
# Explode the 'skills' array column using posexplode_outer
exploded_df = df.select("id", "name", posexplode_outer("skills").alias("position", "skill"))
exploded_df.show()
