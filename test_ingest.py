# Databricks notebook source
df1 = spark.read.format("csv").option("header", "true").load("dbfs:/databricks-datasets/COVID/covid-19-data/")
df1.show(100)

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog if not exists sunshine;
# MAGIC use catalog sunshine;
# MAGIC create database if not exists sunschema;
# MAGIC use database sunschema;

# COMMAND ----------

from pyspark.sql.functions import *


df1 = spark.read.format("csv").option("header", "true").load("dbfs:/databricks-datasets/COVID/ESRI_hospital_beds/")
df1.count()


#df = spark.sql("SELECT * FROM csv.`/Volumes/sunshine/sunschema/datavolume/product.csv`")

#df.show(100)

df1.write.mode("overwrite").saveAsTable("hospitalbeds")
df2 = spark.read.format("csv").option("header", "true").load("dbfs:/databricks-datasets/COVID/coronavirusdataset/")
df2.write.mode("overwrite").saveAsTable("coronadata")
df3 = spark.read.format("csv").option("header", "true").load("dbfs:/databricks-datasets/COVID/covid-19-data/")
df3.write.mode("overwrite").saveAsTable("covid19data")
