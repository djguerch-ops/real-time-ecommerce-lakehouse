# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Transform
# MAGIC
# MAGIC Cleans and validates the Bronze layer: proper timestamp typing,
# MAGIC deduplication on `event_id`, and filtering of invalid records.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formation.bronze.events

# COMMAND ----------

from pyspark.sql.functions import *

# Read Bronze
df = spark.table("formation.bronze.events")

# Silver transformations:
# - cast event_ts from string to a proper timestamp
# - drop duplicate events (same event_id ingested twice)
# - drop rows with an invalid/unparseable timestamp
# - keep only known event types (defensive check against schema drift)
# - drop rows missing a user_id
df_silver = (
    df
    .withColumn("event_ts", to_timestamp("event_ts"))
    .dropDuplicates(["event_id"])
    .filter(col("event_ts").isNotNull())
    .filter(col("event_type").isin("page_view", "add_to_cart", "order_created"))
    .filter(col("user_id").isNotNull())
)

# Ensure the silver schema exists
spark.sql("CREATE SCHEMA IF NOT EXISTS formation.silver")

# Write Silver
df_silver.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("formation.silver.events")

print(f"Lignes Silver : {df_silver.count()}")
df_silver.show(5)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formation.silver.events
