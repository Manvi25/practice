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

# collect action
all_elements = rdd.collect()
print("All Elements:", all_elements)

# take action
first_three_elements = rdd.take(3)
print("First Three Elements:", first_three_elements)

# top action
top_three_elements = rdd.top(3)
print("Top Three Elements:", top_three_elements)

# reduce action
from operator import add
sum_of_elements = rdd.reduce(add)
print("Sum of Elements:", sum_of_elements)

# countByKey action
pair_rdd = sc.parallelize([(1, 'a'), (2, 'b'), (1, 'c'), (2, 'd'), (3, 'e')])
count_by_key = pair_rdd.countByKey()
print("Count by Key:", count_by_key)

# countByValue action
count_by_value = rdd.countByValue()
print("Count by Value:", count_by_value)

# fold action
initial_value = 0
sum_fold = rdd.fold(initial_value, add)
print("Sum using fold action:", sum_fold)

# range action
rdd_range = sc.range(1, 10, 2)
print("Range RDD:", rdd_range.collect())



