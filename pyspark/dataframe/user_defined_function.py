from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
spark = SparkSession.builder.appName("UDF Example").getOrCreate()

data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
df = spark.createDataFrame(data, ["name", "age"])
def add_prefix(name):
    return "Mr. " + name

add_prefix_udf = udf(add_prefix, StringType())

df_with_prefix = df.withColumn("prefixed_name", add_prefix_udf(df["name"]))
df_with_prefix.show()
