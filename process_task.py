# Databricks notebook source
df = spark.sql("select * from sunshine.sunschema.customer")
df.groupBy("last_name").count().write.saveAsTable("sunshine.sunschema.customer_groupedByLastName")
