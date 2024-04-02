from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Unpivot Example").getOrCreate()
data = [("Alice", 90, 85), ("Bob", 88, 92)]
df = spark.createDataFrame(data, ["Name", "Math", "Science"])
unpivoted_df = df.selectExpr("Name", "stack(2, 'Math', Math, 'Science', Science) as (Subject, Score)")
unpivoted_df.show()
