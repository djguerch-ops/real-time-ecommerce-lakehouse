# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Streaming Ingestion (V2)
# MAGIC
# MAGIC Ingests events from the `streaming/` prefix, fed in near real-time by
# MAGIC Kinesis → Lambda. Same schema and Auto Loader pattern as the V1 batch
# MAGIC notebook, but pointed at a different S3 prefix and writing to a
# MAGIC separate Bronze table — so V1 and V2 can be compared side by side.
# MAGIC
# MAGIC Uses `trigger(availableNow=True)` (on-demand, not continuous) for cost
# MAGIC reasons — see the README section "V2 — Real-time ingestion" for the
# MAGIC continuous-trigger version that was tested and the trade-off discussion.

# COMMAND ----------

s3_path = "s3://rtl-dev-raw-563683519302/streaming/events/"
checkpoint_path = "s3://rtl-dev-raw-563683519302/checkpoints/bronze_streaming/"

print(f"Source : {s3_path}")
print(f"Checkpoint : {checkpoint_path}")

# COMMAND ----------

from pyspark.sql.types import *

event_schema = StructType([
    StructField("event_id",    StringType(),  False),
    StructField("event_type",  StringType(),  False),
    StructField("event_ts",    StringType(),  False),
    StructField("user_id",     StringType(),  False),
    StructField("session_id",  StringType(),  False),
    StructField("product_id",  StringType(),  True),
    StructField("order_id",    StringType(),  True),
    StructField("device_type", StringType(),  False),
    StructField("country",     StringType(),  False),
    StructField("amount",      DoubleType(),  True),
    StructField("currency",    StringType(),  False),
])

print("Schéma défini ✓")

# COMMAND ----------

df_bronze_streaming = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.useNotifications", "false")
    .schema(event_schema)
    .load(s3_path)
)
print("Auto Loader (streaming) configuré ✓")

# COMMAND ----------

spark.sql("USE CATALOG formation")
spark.sql("USE SCHEMA bronze")

(
    df_bronze_streaming
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable("formation.bronze.events_streaming")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity check

# COMMAND ----------

df = spark.read.table("formation.bronze.events_streaming")
print(f"Nombre de lignes (streaming) : {df.count()}")
df.orderBy("event_ts").show(10)
