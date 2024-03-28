from pyspark.sql import *

spark = SparkSession.builder.appName('Manvi').getOrCreate()
df = spark.read.option("multiline", "true").json("sample4.json")
df.show(truncate=False)
df.printSchema()