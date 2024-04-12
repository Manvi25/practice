-- Databricks notebook source
-- MAGIC %python
-- MAGIC data=[(1,'manvi','female'),(2,'sakshi','female'),(3,'rahul','male'),(4,'ishita','female'),(5,'rohan','male')]
-- MAGIC cols=['id','name','gender']
-- MAGIC df = spark.createDataFrame(data, cols)
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.createOrReplaceTempView('persons')

-- COMMAND ----------

select * from persons

-- COMMAND ----------

create widget dropdown gender default 'female' choices select gender from persons

-- COMMAND ----------


