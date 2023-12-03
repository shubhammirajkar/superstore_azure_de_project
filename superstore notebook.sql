-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.fs.mount(
-- MAGIC   source="wasbs://raw@blobstoragesuperstore.blob.core.windows.net",
-- MAGIC   mount_point= "/mnt/raw",
-- MAGIC   extra_configs={"fs.azure.account.key.blobstoragesuperstore.blob.core.windows.net":"wRB20TXymBgQuqmYL4EfuCB379FOwNzJqiXyWC0vseumsDTJxKELiGJ9v5ffH6ufT4pRmMm7nBiK+AStGsTBxw=="}
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import *
-- MAGIC import urllib
-- MAGIC
-- MAGIC df= spark.read.format("csv") \
-- MAGIC .options(header= True) \
-- MAGIC .options(delimiter = ",") \
-- MAGIC .options(inferSchema = True) \
-- MAGIC .load("/mnt/raw/superstore.csv")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.mode("overwrite").saveAsTable("superstore")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = df.withColumnRenamed("Product_ ID","Product_ID")
-- MAGIC df = df.withColumnRenamed("Ship _Date","Ship_Date")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.columns

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.mode("overwrite").saveAsTable("superstore")

-- COMMAND ----------

select * from superstore

-- COMMAND ----------

select sum(Profit),city from superstore
group by 2

-- COMMAND ----------

select sum(Sales),Region from superstore
group by 2
