from pyspark import SparkContext
sc = SparkContext("local", "RDD actions example")

# Create an RDD
rdd = sc.parallelize([1, 2, 3, 4, 5])

# collect action
collected_rdd = rdd.collect()
print("Collected RDD:", collected_rdd)

# take action
taken_rdd = rdd.take(3)
print("Taken RDD:", taken_rdd)

# top action
top_rdd = rdd.top(3)
print("Top RDD:", top_rdd)

# count action
count_rdd = rdd.count()
print("Count RDD:", count_rdd)

# min action
min_rdd = rdd.min()
print("Min RDD:", min_rdd)

# max action
max_rdd = rdd.max()
print("Max RDD:", max_rdd)

# mean action
mean_rdd = rdd.mean()
print("Mean RDD:", mean_rdd)

