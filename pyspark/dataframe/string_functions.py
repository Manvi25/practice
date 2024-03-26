from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create a SparkSession
spark = SparkSession.builder \
    .appName("StringFunctionsExample") \
    .getOrCreate()

# Sample data
data = [("Manvi Khandelwal",), ("  Sakshi  ",), ("Ishita",)]

df = spark.createDataFrame(data, ["name"])

# Using upper() to convert string to uppercase
df_upper = df.select(upper(col("name")).alias("upper_name"))

# Using trim() to remove leading and trailing whitespace
df_trim = df.select(trim(col("name")).alias("trimmed_name"))

# Using ltrim() to remove leading whitespace
df_ltrim = df.select(ltrim(col("name")).alias("left_trimmed_name"))

# Using rtrim() to remove trailing whitespace
df_rtrim = df.select(rtrim(col("name")).alias("right_trimmed_name"))

# Using translate() to replace characters in a string
df_translate = df.select(translate(col("name"), " ", "_").alias("translated_name"))

# Using substring_index() to get substring before a delimiter
df_substring_index = df.select(substring_index(col("name"), " ", 1).alias("first_name"))

# Using substring() to get a substring
df_substring = df.select(substring(col("name"), 2, 3).alias("substring_name"))

# Using split() to split a string into an array
df_split = df.select(split(col("name"), " ").alias("name_array"))

# Using repeat() to repeat a string n times
df_repeat = df.select(repeat(col("name"), 2).alias("repeated_name"))

# Using rpad() to right pad a string with specified characters
df_rpad = df.select(rpad(col("name"), 10, "_").alias("right_padded_name"))

# Using lpad() to left pad a string with specified characters
df_lpad = df.select(lpad(col("name"), 10, "_").alias("left_padded_name"))

# Using regex_replace() to replace a pattern in a string
df_regex_replace = df.select(regexp_replace(col("name"), "\\s+", "_").alias("regex_replaced_name"))

# Using lower() to convert string to lowercase
df_lower = df.select(lower(col("name")).alias("lower_name"))

# Using regex_extract() to extract a pattern from a string
df_regex_extract = df.select(regexp_extract(col("name"), "\\w+", 0).alias("regex_extracted_name"))

# Using length() to get the length of a string
df_length = df.select(length(col("name")).alias("name_length"))

# Show results
df_upper.show()
df_trim.show()
df_ltrim.show()
df_rtrim.show()
df_translate.show()
df_substring_index.show()
df_substring.show()
df_split.show()
df_repeat.show()
df_rpad.show()
df_lpad.show()
df_regex_replace.show()
df_lower.show()
df_regex_extract.show()
df_length.show()
