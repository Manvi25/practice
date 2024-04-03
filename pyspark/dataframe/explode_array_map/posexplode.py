from pyspark.sql import SparkSession
from pyspark.sql.functions import posexplode

spark = SparkSession.builder \
    .appName("PosExplode DataFrame Example") \
    .getOrCreate()
data = [(1, "John", ["Python", "Java", "SQL"]),
        (2, "Alice", ["Scala", "Python"]),
        (3,"Manvi",None)]
columns = ["id", "name", "skills"]
df = spark.createDataFrame(data, columns)
df.show()
# posexplode function
exploded_df = df.select("id", "name", posexplode("skills").alias("position", "skill"))
exploded_df.show()
