from pyspark.sql import SparkSession; from datetime import date
spark = SparkSession.builder.getOrCreate()
df= spark.createDataFrame([
    ("red",1,"apple", date(2021,1,1)),
    ("black",2,"grape", date(2021,2,1)),
    ("yellow",3,"banana", date(2021,3,1))
], schema= "color string, sr_no long, fruit string,dates date")
print(df)
print(df.show())
#to show only 2 rows
print(df.show(2))



