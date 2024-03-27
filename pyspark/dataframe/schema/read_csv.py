from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("read csv").getOrCreate()
flight_df = spark.read.format("csv")\
    .option("header","false")\
    .option("inferschema","false")\
    .option("mode","FAILFAST")\
    .load("2010-summary.csv")


