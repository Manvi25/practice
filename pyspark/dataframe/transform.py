from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
spark = SparkSession.builder \
    .appName("Transform Example") \
    .getOrCreate()
data = [("Alice", ["Math", "Science"]),
        ("Bob", ["Physics", "Chemistry"]),
        ("Charlie", ["Biology", "Geography"])]
schema = StructType([
    StructField("name", StringType(), True),
    StructField("subjects", ArrayType(StringType()), True)
])
df = spark.createDataFrame(data, schema)
def transform_subjects(subjects):
    # Convert subjects to uppercase
    return [subject.upper() for subject in subjects]

#transformation using transform()
transformed_df = df.withColumn("transformed_subjects", expr("transform(subjects, x -> upper(x))"))
transformed_df.show(truncate=False)
