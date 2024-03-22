from pyspark import SparkContext

# Initialize SparkContext
sc = SparkContext("local")

# Creating RDDs with sample data
data1 = [(1, 'Manvi'), (2, 'Sakshi'), (3, 'Ishita')]
data2 = [(1, 'Rishika'), (2, 'Kumud'), (4, 'Arushi')]

# Creating RDDs from sample data
rdd1 = sc.parallelize(data1)
rdd2 = sc.parallelize(data2)

# Performing different types of joins
# Inner join
inner_join_result = rdd1.join(rdd2)
print("Inner Join Result:", inner_join_result.collect())

# Left outer join
left_outer_join_result = rdd1.leftOuterJoin(rdd2)
print("Left Outer Join Result:", left_outer_join_result.collect())

# Right outer join
right_outer_join_result = rdd1.rightOuterJoin(rdd2)
print("Right Outer Join Result:", right_outer_join_result.collect())

# Full outer join
full_outer_join_result = rdd1.fullOuterJoin(rdd2)
print("Full Outer Join Result:", full_outer_join_result.collect())

# Cross join
cross_join_result = rdd1.cartesian(rdd2)
print("Cross Join Result:", cross_join_result.collect())
