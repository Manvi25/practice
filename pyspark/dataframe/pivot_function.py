from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
spark = SparkSession.builder.appName("Pivot with Custom Schema").getOrCreate()
schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Subject", StringType(), True),
    StructField("Score", IntegerType(), True)
])
data = [("Alice", "Math", 90), ("Alice", "Science", 85),
        ("Bob", "Math", 88), ("Bob", "Science", 92)]
df = spark.createDataFrame(data, schema)
#pivot function
pivot_df = df.groupBy("Name").pivot("Subject").sum("Score")
pivot_df.show()