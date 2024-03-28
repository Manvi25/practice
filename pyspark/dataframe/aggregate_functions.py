from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, avg, collect_list, collect_set, countDistinct, count, first, last

# Create a SparkSession
spark = SparkSession.builder.appName("AggregateFunctions").getOrCreate()

# Sample DataFrame
data = [("John", 100),
        ("Jane", 200),
        ("John", 300),
        ("Jane", 400),
        ("John", 500)]
df = spark.createDataFrame(data, ["Name", "Salary"])

# Using aggregate functions
mean_salary = df.select(mean("Salary")).collect()[0][0]
avg_salary = df.select(avg("Salary")).collect()[0][0]
list_of_salaries = df.groupby("Name").agg(collect_list("Salary")).collect()[0][1]
set_of_salaries = df.groupby("Name").agg(collect_set("Salary")).collect()[0][1]
distinct_names = df.select(countDistinct("Name")).collect()[0][0]
total_count = df.count()
first_name = df.groupby().agg(first("Name")).collect()[0][0]
last_name = df.groupby().agg(last("Name")).collect()[0][0]

# Display the results
print("Mean Salary:", mean_salary)
print("Average Salary:", avg_salary)
print("List of Salaries:", list_of_salaries)
print("Set of Salaries:", set_of_salaries)
print("Distinct Names Count:", distinct_names)
print("Total Count:", total_count)
print("First Name:", first_name)
print("Last Name:", last_name)
