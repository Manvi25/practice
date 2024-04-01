from pyspark.sql import *

spark = SparkSession.builder.appName('Manvi').getOrCreate()
df = spark.read.option("multiline", "true").json(r"C:\Users\ManviKhandelwal\Downloads\practice.json")
df.show(truncate=False)
df.printSchema()