from pyspark import SparkContext
sc = SparkContext("local", "RDD actions example")

# Create an RDD
rdd = sc.parallelize([1, 2, 3, 4, 5])

# min action
min_rdd = rdd.min()
print("Min RDD:", min_rdd)

# max action
max_rdd = rdd.max()
print("Max RDD:", max_rdd)

# mean action
mean_rdd = rdd.mean()
print("Mean RDD:", mean_rdd)

