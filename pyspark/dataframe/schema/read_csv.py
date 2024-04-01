from pyspark.sql import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType , DateType
spark = SparkSession.builder.appName('Manvi').getOrCreate()
schema=StructType([
    StructField("Dest_countryname",StringType()),
    StructField("origin_countryname", StringType()),
    StructField("count", IntegerType()),
])

#using custom schema reading a csv file
custom_schema_df = spark.read.csv(path=r'C:\Users\ManviKhandelwal\Downloads\2010-summary.csv' ,
                                  schema=schema , header=True)
custom_schema_df.show()
custom_schema_df.printSchema()
