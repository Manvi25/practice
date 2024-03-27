from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("casting") \
    .getOrCreate()

data = [("Manvi", 22),
        ("Sakshi", 23),
        ("Ishita", 21)]
df = spark.createDataFrame(data, ["Name", "Age"])
print("Before casting:")
df.printSchema()
#cast()
df = df.withColumn("Age", col("Age").cast("string"))
print("After casting:")
df.printSchema()
df.show()