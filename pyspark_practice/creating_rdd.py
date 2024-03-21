import pyspark
from pyspark.sql import SparkSession
#creating an object spark and calling the sparksession
spark = SparkSession.builder.getOrCreate()

#creating the rdd
rdds = spark.sparkContext.parallelize([("mumbai",1),("jaipur",2),("chennai",3)])

#using collect method
print(rdds.collect())

#using count method
print(rdds.count())