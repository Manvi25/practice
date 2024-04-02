from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql.functions import array_contains
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Array Functions").getOrCreate()

data = [("Manvi", ["Physics", "Chemistry", "Maths"]),
        ("Sakshi", ["Physics", "Maths"]),
        ("Kumud", ["Chemistry", "Biology", "Physics"]),
        ("Ishita", ["Accounts", "Biology", "Chemisty"]),
        ("Priya ", ["Chemistry", "Physics"])]

schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Subject", ArrayType(StringType()), True)
])

df = spark.createDataFrame(data, schema)
result_df = df.withColumn("contains_physics", array_contains("Subject","Physics"))
result_df.show()