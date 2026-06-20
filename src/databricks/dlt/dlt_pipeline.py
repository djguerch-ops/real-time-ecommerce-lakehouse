# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Pipeline — E-commerce Events with Data Quality Expectations
# MAGIC
# MAGIC ## Relationship to the V1 pipeline
# MAGIC
# MAGIC This project demonstrates **two different orchestration approaches** for the same
# MAGIC Bronze → Silver → Gold logic, intentionally, to compare them:
# MAGIC
# MAGIC - **Databricks Job** (`ecommerce-pipeline-v1`) — chains the existing notebooks
# MAGIC   (`01_bronze_ingestion` → `02_silver_transform` → `03_gold_kpi`) exactly as written.
# MAGIC   Dependencies between tasks are declared explicitly by the developer in the Job UI.
# MAGIC   No code changes needed — this is the simplest way to schedule an existing pipeline.
# MAGIC
# MAGIC - **This DLT pipeline** — the same logic rewritten declaratively. Dependencies between
# MAGIC   tables are inferred automatically from the code itself (DLT sees that
# MAGIC   `dlt_silver_events` reads `dlt_bronze_events` and orders execution accordingly — no
# MAGIC   manual task graph). The key addition is `@dlt.expect_or_drop(...)`: data quality
# MAGIC   rules enforced inline, with row counts (passed/dropped, per rule) tracked
# MAGIC   automatically and visible in DLT's built-in pipeline graph — no custom counting code.
# MAGIC
# MAGIC Tables here are named `dlt_*` and live in a separate schema, so this pipeline can run
# MAGIC independently without conflicting with the V1 tables.
# MAGIC
# MAGIC Reads from the same V1 batch source (`s3://.../events/`) as `01_bronze_ingestion`,
# MAGIC so results are directly comparable.

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, to_timestamp, sum as spark_sum, count as spark_count, avg, round as spark_round, countDistinct, when
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Event schema
# MAGIC Same explicit schema as the V1 notebooks — enforced here too, so malformed records
# MAGIC are caught at the earliest possible point.

# COMMAND ----------

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

S3_PATH = "s3://rtl-dev-raw-563683519302/events/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze — raw ingestion
# MAGIC
# MAGIC Expectations here are intentionally loose: Bronze should accept anything that's
# MAGIC structurally readable. We only enforce that the record isn't completely empty.

# COMMAND ----------

@dlt.table(
    name="dlt_bronze_events",
    comment="Raw e-commerce events ingested via Auto Loader, schema-enforced but not yet validated.",
    table_properties={"quality": "bronze"},
)
@dlt.expect_or_drop("non_null_event_id", "event_id IS NOT NULL")
def dlt_bronze_events():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.useNotifications", "false")
        .schema(event_schema)
        .load(S3_PATH)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver — cleaned and validated
# MAGIC
# MAGIC This is where `expect_or_drop` does the real work: any row failing one of these
# MAGIC rules is dropped before reaching Silver, and DLT records exactly how many rows
# MAGIC were dropped per rule — visible in the pipeline graph without extra code.

# COMMAND ----------

@dlt.table(
    name="dlt_silver_events",
    comment="Cleaned, deduplicated, and validated events — only rows passing all quality expectations.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_event_type", "event_type IN ('page_view', 'add_to_cart', 'order_created')")
@dlt.expect_or_drop("valid_device_type", "device_type IN ('mobile', 'desktop', 'tablet')")
@dlt.expect_or_drop("non_null_user_id", "user_id IS NOT NULL")
@dlt.expect_or_drop("non_null_timestamp", "event_ts IS NOT NULL")
@dlt.expect_or_drop("order_has_amount", "event_type != 'order_created' OR amount IS NOT NULL")
@dlt.expect_or_drop("positive_amount", "amount IS NULL OR amount > 0")
def dlt_silver_events():
    return (
        dlt.read_stream("dlt_bronze_events")
        .withColumn("event_ts", to_timestamp("event_ts"))
        .dropDuplicates(["event_id"])
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold — business KPIs
# MAGIC
# MAGIC Same four KPI tables as the manual `03_gold_kpi` notebook, now built declaratively
# MAGIC from the validated Silver layer. Gold tables in DLT are typically batch
# MAGIC (materialized) views rather than streaming, since aggregates need a complete
# MAGIC snapshot to be meaningful.

# COMMAND ----------

@dlt.table(
    name="dlt_gold_conversion_rate",
    comment="Per-user conversion: did the user place at least one order?",
    table_properties={"quality": "gold"},
)
def dlt_gold_conversion_rate():
    df = dlt.read("dlt_silver_events")
    return (
        df.groupBy("user_id")
        .agg(
            countDistinct(when(col("event_type") == "page_view", col("event_id"))).alias("nb_page_views"),
            countDistinct(when(col("event_type") == "add_to_cart", col("event_id"))).alias("nb_add_to_cart"),
            countDistinct(when(col("event_type") == "order_created", col("event_id"))).alias("nb_orders"),
        )
        .withColumn("a_converti", col("nb_orders") > 0)
    )

# COMMAND ----------

@dlt.table(
    name="dlt_gold_ca_par_pays",
    comment="Revenue, order count, and average basket per country.",
    table_properties={"quality": "gold"},
)
def dlt_gold_ca_par_pays():
    df = dlt.read("dlt_silver_events")
    return (
        df.filter(col("event_type") == "order_created")
        .groupBy("country")
        .agg(
            spark_round(spark_sum("amount"), 2).alias("chiffre_affaires"),
            spark_count("order_id").alias("nb_commandes"),
            spark_round(avg("amount"), 2).alias("panier_moyen"),
        )
        .orderBy(col("chiffre_affaires").desc())
    )

# COMMAND ----------

@dlt.table(
    name="dlt_gold_top_produits",
    comment="Top 10 products by cart additions.",
    table_properties={"quality": "gold"},
)
def dlt_gold_top_produits():
    df = dlt.read("dlt_silver_events")
    return (
        df.filter(col("event_type") == "add_to_cart")
        .groupBy("product_id")
        .agg(spark_count("*").alias("nb_ajouts_panier"))
        .orderBy(col("nb_ajouts_panier").desc())
        .limit(10)
    )

# COMMAND ----------

@dlt.table(
    name="dlt_gold_distribution_device",
    comment="Event distribution across mobile / desktop / tablet.",
    table_properties={"quality": "gold"},
)
def dlt_gold_distribution_device():
    df = dlt.read("dlt_silver_events")
    total = df.count()
    return (
        df.groupBy("device_type")
        .agg(spark_count("*").alias("nb_events"))
        .withColumn("pourcentage", spark_round(col("nb_events") / total * 100, 2))
        .orderBy(col("nb_events").desc())
    )
