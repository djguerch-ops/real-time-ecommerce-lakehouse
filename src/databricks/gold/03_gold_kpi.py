# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — Business KPIs
# MAGIC
# MAGIC Builds four business-facing KPI tables from the Silver layer:
# MAGIC - `conversion_rate` — per-user conversion (did they place an order?)
# MAGIC - `ca_par_pays` — revenue, order count and average basket per country
# MAGIC - `top_produits` — top 10 products by cart additions
# MAGIC - `distribution_device` — event distribution across mobile/desktop/tablet
# MAGIC
# MAGIC These tables back the Databricks SQL Dashboard.

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS formation.gold")
print("Schema gold créé ✓")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conversion rate
# MAGIC Conversion is computed **per user**, not per raw event: for each user we
# MAGIC count distinct events of each type, then flag whether they placed at
# MAGIC least one order. The overall rate is the share of users who converted.

# COMMAND ----------

from pyspark.sql.functions import *

df_silver = spark.table("formation.silver.events")

df_conversion = (
    df_silver
    .groupBy("user_id")
    .agg(
        countDistinct(when(col("event_type") == "page_view", col("event_id"))).alias("nb_page_views"),
        countDistinct(when(col("event_type") == "add_to_cart", col("event_id"))).alias("nb_add_to_cart"),
        countDistinct(when(col("event_type") == "order_created", col("event_id"))).alias("nb_orders")
    )
    .withColumn("a_converti", col("nb_orders") > 0)
)

nb_users_total     = df_conversion.count()
nb_users_convertis = df_conversion.filter("a_converti = true").count()

# Cast to Python float explicitly — count() can return non-primitive numeric
# types that conflict with the built-in round() once `from ...functions import *`
# has shadowed it with pyspark.sql.functions.round.
taux = float(nb_users_convertis) / float(nb_users_total) * 100
taux = int(taux * 100) / 100

print(f"Users totaux       : {nb_users_total}")
print(f"Users convertis    : {nb_users_convertis}")
print(f"Taux de conversion : {taux}%")

df_conversion.write.mode("overwrite").saveAsTable("formation.gold.conversion_rate")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Revenue by country

# COMMAND ----------

import pyspark.sql.functions as F

df_silver = spark.table("formation.silver.events")

df_ca_pays = (
    df_silver
    .filter(F.col("event_type") == "order_created")
    .groupBy("country")
    .agg(
        F.round(F.sum("amount"), 2).alias("chiffre_affaires"),
        F.count("order_id").alias("nb_commandes"),
        F.round(F.avg("amount"), 2).alias("panier_moyen")
    )
    .orderBy(F.col("chiffre_affaires").desc())
)

df_ca_pays.show()
df_ca_pays.write.mode("overwrite").saveAsTable("formation.gold.ca_par_pays")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Top products by cart additions

# COMMAND ----------

df_top_produits = (
    df_silver
    .filter(F.col("event_type") == "add_to_cart")
    .groupBy("product_id")
    .agg(F.count("*").alias("nb_ajouts_panier"))
    .orderBy(F.col("nb_ajouts_panier").desc())
    .limit(10)
)

df_top_produits.show()
df_top_produits.write.mode("overwrite").saveAsTable("formation.gold.top_produits")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Device distribution

# COMMAND ----------

total = df_silver.count()

df_device = (
    df_silver
    .groupBy("device_type")
    .agg(F.count("*").alias("nb_events"))
    .withColumn("pourcentage", F.round(F.col("nb_events") / total * 100, 2))
    .orderBy(F.col("nb_events").desc())
)

df_device.show()
df_device.write.mode("overwrite").saveAsTable("formation.gold.distribution_device")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity check

# COMMAND ----------

print("=== Tables Gold créées ===")
for table in ["conversion_rate", "ca_par_pays", "top_produits", "distribution_device"]:
    count = spark.table(f"formation.gold.{table}").count()
    print(f"formation.gold.{table} : {count} lignes ✓")
