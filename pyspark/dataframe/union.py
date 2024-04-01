from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark= SparkSession.builder.appName("union example").getOrCreate()
schema= StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])
data1= [("Manvi",23),("Sakshi",25)]
df1= spark.createDataFrame(data1, schema)
data2= [("Manvi",23),("Manasvi",20)]
df2= spark.createDataFrame(data2,schema)

#union()
union_df= df1.union(df2)
union_df.show()

#unionAll()
unionall_df= df1.unionAll(df2)
unionall_df.show()

data3=[("Manvi", "Jaipur"),("Sakshi","Mumbai")]
schema2= StructType([
    StructField("name", StringType(), True),
    StructField("city", StringType(), True)
])
df3 = spark.createDataFrame(data3, schema2)

#unionbyname()
unionbyname= df1.unionByName(df3, allowMissingColumns=True)
unionbyname.show()