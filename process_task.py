# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog sunshine;
# MAGIC use schema sunschema;

# COMMAND ----------



df = (spark.sql("select a.*, b.* from (select  fips cfips, max(NUM_LICENSED_BEDS) NUM_LICENSED_BEDS, max(NUM_STAFFED_BEDS) NUM_STAFFED_BEDS,max(NUM_ICU_BEDS) NUM_ICU_BEDS, max(ADULT_ICU_BEDS) ADULT_ICU_BEDS, max  \
      (avg_ventilator_usage) avg_ventilator_usage from sunshine.sunschema.hospitalbeds   group by fips) b left join (select distinct fips, hospital_name, hq_address, hq_city, \
       county_name,hq_state,hq_zip_code from sunshine.sunschema.hospitalbeds) a on  b.cfips = a.fips") \
      ).drop("cfips")
df.write.mode("overwrite").saveAsTable("sunshine.sunschema.hospitals_groupedByfips")
