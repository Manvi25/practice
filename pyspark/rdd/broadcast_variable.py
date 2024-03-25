import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
spark = SparkSession.builder \
    .appName("broadcast variable") \
    .getOrCreate()
transaction= [
    (100,"cosmetic",150),
    (200,"shirt",250),
    (300,"trouser",400),
    (400,"belt",70),
    (500,"socks",25),
    (100,"cosmetic",500),
    (200,"shoe",300),
    (300,"socks",70),
    (400,"shirt",300),
    (500,"apparel",250)
]
transactionDF= spark.createDataFrame(data= transaction, schema = ['store_id','item','amount'])
transactionDF.show()

store= [
    (100,"store_london"),
    (200,"store_paris"),
    (300,"store_oslo"),
    (400,"store_dubai"),
    (500,"store_frankfurt")
]
storeDF= spark.createDataFrame(data= store, schema= ['store_id','store_name'])
storeDF.show()

joinDF= transactionDF.join(broadcast(storeDF), transactionDF['store_id']==storeDF['store_id'])
joinDF.show()
joinDF.explain()