from pyspark.sql.functions import row_number, rank, dense_rank, lead, lag
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("window functions").getOrCreate()
data= [
    ("maheer","hr",2000),
    ("wafa","it",3000),
    ("asi","hr",1500),
    ("annu","payroll",3500),
    ("shakti","it",3000),
    ("pradeep","it",4000),
    ("kranthi","payroll",2000),
    ("himanshu","it",2000),
    ("bhargava","hr",2000),
    ("martin","it",2500),
]
schema= ["name","dep","salary"]
df= spark.createDataFrame(data, schema)
df.sort("dep").show()

window= Window.partitionBy("dep").orderBy("salary")
(df.withColumn("rownumber",row_number().over(window)). \
    withColumn("rank",rank().over(window)).\
    withColumn("denserank",dense_rank().over(window)).\
    withColumn("lead_salary",lead("salary",1).over(window)).\
    withColumn("lag_salary",lag("salary",1).over(window)).show())