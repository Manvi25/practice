from pyspark import SparkContext
# Creating a SparkContext
sc = SparkContext("local", "RDD transformations example")
# Create an RDD
rdd = sc.parallelize([1, 2, 3, 4, 5])

# map transformation
mapped_rdd = rdd.map(lambda x: x * 2)
print("Mapped RDD:", mapped_rdd.collect())

# flatMap transformation
flat_mapped_rdd = rdd.flatMap(lambda x: [x, x])
print("FlatMapped RDD:", flat_mapped_rdd.collect())

# filter transformation
filtered_rdd = rdd.filter(lambda x: x % 2 == 0)
print("Filtered RDD:", filtered_rdd.collect())

# distinct transformation
distinct_rdd = rdd.union(sc.parallelize([3, 4, 5, 6])).distinct()
print("Distinct RDD:", distinct_rdd.collect())


# mapPartitions transformation
mapped_partitions_rdd = rdd.mapPartitions(lambda partition: [x * 2 for x in partition])
print("Mapped Partitions RDD:", mapped_partitions_rdd.collect())

# mapPartitionsWithIndex transformation
mapped_partitions_with_index_rdd = rdd.mapPartitionsWithIndex(lambda idx, partition: [(idx, x) for x in partition])
print("Mapped Partitions With Index RDD:", mapped_partitions_with_index_rdd.collect())





