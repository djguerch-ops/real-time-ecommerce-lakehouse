# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Ingestion
# MAGIC
# MAGIC Ingests raw e-commerce events from S3 into the Bronze Delta table using
# MAGIC Databricks Auto Loader. Authentication to S3 is handled entirely through
# MAGIC Unity Catalog (External Location `rtl-dev-raw`) — no credentials are
# MAGIC referenced in this notebook.

# COMMAND ----------

# Paths resolved via the Unity Catalog External Location
s3_path = "s3://rtl-dev-raw-563683519302/events/"
checkpoint_path = "s3://rtl-dev-raw-563683519302/checkpoints/bronze/"

print(f"Source : {s3_path}")
print(f"Checkpoint : {checkpoint_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Event schema
# MAGIC Explicit schema matching `src/event_generator/ecommerce_event.json`.
# MAGIC Enforcing it here lets Auto Loader reject/flag unexpected data shapes
# MAGIC early, instead of silently inferring types.

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

# MAGIC %md
# MAGIC ## Auto Loader
# MAGIC `cloudFiles.useNotifications` is set to `false` — V1 uses directory
# MAGIC listing rather than S3 event notifications (SQS/SNS). This keeps the
# MAGIC IAM permission surface smaller; event-driven notifications are a V2
# MAGIC enhancement.

# COMMAND ----------

df_bronze = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.useNotifications", "false")
    .schema(event_schema)
    .load(s3_path)
)
print("Auto Loader configuré ✓")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Bronze
# MAGIC `trigger(availableNow=True)` processes all currently available files
# MAGIC once and stops — batch-style incremental processing, sufficient for V1.
# MAGIC Continuous streaming is planned for V2 with Kinesis as the source.

# COMMAND ----------

spark.sql("USE CATALOG formation")
spark.sql("USE SCHEMA bronze")

(
    df_bronze
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable("formation.bronze.events")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formation.bronze.events

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity check

# COMMAND ----------

df = spark.read.table("formation.bronze.events")
print(f"Nombre de lignes : {df.count()}")
df.show(5)
