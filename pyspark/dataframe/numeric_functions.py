from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, min, max,round,abs
spark = SparkSession.builder.appName("Numeric Functions").getOrCreate()
data=([1,10],[2,20],[3,30],[4,40],[5,50])
df= spark.createDataFrame(data, ["id","value"])

#sum of values
total_sum= df.select(sum("value")).collect()[0][0]
print("Sum:",total_sum)

#avg of values
average= df.select(avg("value")).collect()[0][0]
print("Average:", average)

#min of values
minimum= df.select(min("value")).collect()[0][0]
print("Minimum:", minimum)

#max of values
maximum= df.select(max("value")).collect()[0][0]
print("Maximum:", maximum)

#absolute of values
absolute= df.select(abs("value").alias("abs_value"))
absolute.show()