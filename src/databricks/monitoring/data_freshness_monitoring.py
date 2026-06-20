# Databricks notebook source
# MAGIC %md
# MAGIC # Data Freshness Monitoring
# MAGIC
# MAGIC Computes observability metrics for the pipeline: how long ago each layer was last
# MAGIC refreshed, how many rows landed in the most recent run, and a simple staleness
# MAGIC status. Designed to run as the last task of the `ecommerce-pipeline-v1` Job, after
# MAGIC `gold_kpi` — so a failure here doesn't block the KPI tables themselves, but still
# MAGIC surfaces if something's gone stale.
# MAGIC
# MAGIC In production this would back an alert ("if status = STALE, page someone") or a
# MAGIC small monitoring dashboard. Here, it's a queryable table — the same data an alert
# MAGIC system would read from.

# COMMAND ----------

from pyspark.sql.functions import (
    col, current_timestamp, max as spark_max, count as spark_count,
    unix_timestamp, round as spark_round, lit, when, to_timestamp
)

spark.sql("CREATE SCHEMA IF NOT EXISTS formation.monitoring")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Freshness thresholds
# MAGIC
# MAGIC This project's source data is generated manually/on-demand (`producer.py`), not on
# MAGIC a continuous daily schedule — so thresholds are set generously to reflect that
# MAGIC reality, rather than assuming a pipeline that's expected to see new data every day.
# MAGIC In a production system with a real continuous source, these would be much tighter
# MAGIC (e.g. 24h / 48h, as a daily-batch pipeline would expect).

# COMMAND ----------

FRESHNESS_OK_DAYS = 14
FRESHNESS_WARNING_DAYS = 30

FRESHNESS_OK_HOURS = FRESHNESS_OK_DAYS * 24
FRESHNESS_WARNING_HOURS = FRESHNESS_WARNING_DAYS * 24

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute freshness per layer
# MAGIC
# MAGIC For each table, we look at the most recent `event_ts` actually present in the data
# MAGIC (not just when the job ran) — this catches a job that "succeeds" but processes an
# MAGIC empty or stale source file, which a simple "did the notebook run" check would miss.

# COMMAND ----------

tables_to_monitor = [
    ("formation.bronze.events", "Bronze"),
    ("formation.silver.events", "Silver"),
]

freshness_rows = []

for table_name, layer in tables_to_monitor:
    df = spark.table(table_name)
    row_count = df.count()

    # event_ts is a String in Bronze (not yet cast) but a proper Timestamp
    # in Silver — to_timestamp() on an already-Timestamp column is a no-op,
    # so this is safe for both without needing per-layer branching.
    last_event_ts = (
        df.select(to_timestamp(col("event_ts")).alias("event_ts"))
        .select(spark_max("event_ts"))
        .collect()[0][0]
    )

    freshness_rows.append((layer, table_name, row_count, last_event_ts))

df_freshness = spark.createDataFrame(
    freshness_rows,
    ["layer", "table_name", "row_count", "last_event_ts"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute staleness status and hours since last data

# COMMAND ----------

df_monitoring = (
    df_freshness
    .withColumn("checked_at", current_timestamp())
    .withColumn(
        "hours_since_last_event",
        spark_round(
            (unix_timestamp("checked_at") - unix_timestamp("last_event_ts")) / 3600, 2
        )
    )
    .withColumn(
        "status",
        when(col("hours_since_last_event") <= FRESHNESS_OK_HOURS, lit("OK"))
        .when(col("hours_since_last_event") <= FRESHNESS_WARNING_HOURS, lit("WARNING"))
        .otherwise(lit("STALE"))
    )
)

df_monitoring.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to monitoring table
# MAGIC
# MAGIC Appended (not overwritten) so the table builds a history of freshness checks over
# MAGIC time — useful to spot a trend (e.g. ingestion gradually getting later each day)
# MAGIC rather than just the latest snapshot.

# COMMAND ----------

df_monitoring.write.mode("append").saveAsTable("formation.monitoring.data_freshness")

print(f"Freshness check recorded — {df_monitoring.count()} layers checked")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fail the task if anything is STALE
# MAGIC
# MAGIC This is what makes the metric actionable rather than just informational: if this
# MAGIC cell raises, the Job task fails, which triggers the failure email notification
# MAGIC already configured on `ecommerce-pipeline-v1`.

# COMMAND ----------

stale_layers = df_monitoring.filter(col("status") == "STALE").collect()

if stale_layers:
    layer_names = [row["layer"] for row in stale_layers]
    raise Exception(f"STALE data detected in layers: {layer_names} — investigate ingestion pipeline")
else:
    print("All monitored layers are within freshness thresholds ✓")
