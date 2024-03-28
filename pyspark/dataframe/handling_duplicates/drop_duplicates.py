from pyspark.sql import SparkSession
spark= SparkSession.builder.appName("drop duplicates").getOrCreate()
data=[
    ("Manvi", "Jaipur", "22"),
    ("Sakshi", "Mumbai","23"),
    ("Ishita","Pune","22"),
    ("Manvi","Bangalore","22")
]
columns= ["Name","City", "Age"]
df= spark.createDataFrame(data= data, schema= columns)
dropdf= df.dropDuplicates(["Name","Age"])
dropdf.show()
