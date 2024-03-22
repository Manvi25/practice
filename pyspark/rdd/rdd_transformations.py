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

# glom transformation
glommed_rdd = rdd.glom()
print("Glommed RDD:", glommed_rdd.collect())

# Union transformation
rdd1 = sc.parallelize([1, 2, 3])
rdd2 = sc.parallelize([4, 5, 6])
union_rdd = rdd1.union(rdd2)
print("Union RDD:", union_rdd.collect())

# Intersection transformation
intersection_rdd = rdd.intersection(sc.parallelize([3, 4, 5]))
print("Intersection RDD:", intersection_rdd.collect())

# Distinct transformation (already provided in the initial code)
print("Distinct RDD:", distinct_rdd.collect())

# groupByKey transformation
pair_rdd = sc.parallelize([(1, 'a'), (2, 'b'), (1, 'c'), (2, 'd'), (3, 'e')])
grouped_rdd = pair_rdd.groupByKey()
print("Grouped RDD:", [(key, list(values)) for key, values in grouped_rdd.collect()])







