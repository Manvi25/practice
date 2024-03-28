from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, current_timestamp, date_add, datediff, year, month, day, to_date, date_format

# Create a SparkSession
spark = SparkSession.builder.appName("DateTimeFunctions").getOrCreate()

# Example DataFrame with dates
data = [("2024-03-25",), ("2024-01-01",)]
df = spark.createDataFrame(data, ["date_str"])

# Using date and time functions
df = df.withColumn("current_date", current_date())  # Current date
df = df.withColumn("current_timestamp", current_timestamp())  # Current timestamp
df = df.withColumn("next_week_date", date_add(df["date_str"], 7))  # Add 7 days to date_str
df = df.withColumn("days_diff", datediff(df["next_week_date"], df["date_str"]))  # Calculate days difference
df = df.withColumn("year", year(df["date_str"]))  # Extract year
df = df.withColumn("month", month(df["date_str"]))  # Extract month
df = df.withColumn("day", day(df["date_str"]))  # Extract day
df = df.withColumn("parsed_date", to_date(df["date_str"]))  # Parse date_str to DateType
df = df.withColumn("formatted_date", date_format(df["parsed_date"], "yyyy-MM-dd"))  # Format date

# Show the result
df.show()
 