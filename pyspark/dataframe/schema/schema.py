from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("FlightData").getOrCreate()
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

flight_schema = StructType(
    [
        StructField("DEST_COUNTRY_NAME", StringType(), True),
        StructField("ORIGIN_COUNTRY_NAME", StringType(), True),
        StructField("flight_count", IntegerType(), True)
    ]
)

flight_df = spark.read.format("csv") \
    .option("header", "false") \
    .option("skipRows", 3) \
    .option("inferschema", "false") \
    .schema(flight_schema) \
    .option("mode", "PERMISSIVE") \
    .load("2010-summary.csv")

flight_df.show(5)
