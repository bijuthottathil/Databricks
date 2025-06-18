# Databricks notebook source
# MAGIC %md
# MAGIC ## Incremental Bronze → Silver → Gold with Merge
# MAGIC This notebook ingests a daily batch of sensor data and updates Gold aggregates using `MERGE INTO`.

# COMMAND ----------

from pyspark.sql.functions import input_file_name, lit
from datetime import datetime

# Parameters
dbutils.widgets.text("ingest_date", "2025-06-18")
target_date = dbutils.widgets.get("ingest_date")
bronze_path = "abfss://oil-lakehouse@<storage_account>.dfs.core.windows.net/medallion/bronze/oil_sensor_data"
silver_path = "abfss://oil-lakehouse@<storage_account>.dfs.core.windows.net/medallion/silver/oil_sensor_data_clean"
gold_path = "abfss://oil-lakehouse@<storage_account>.dfs.core.windows.net/medallion/gold/oil_sensor_summary"

# COMMAND ----------

# Step 1: Bronze - Ingest new CSV
input_path = f"abfss://oil-lakehouse@<storage_account>.dfs.core.windows.net/raw/sensor_data/dt={target_date}/"
df = spark.read.option("header", True).csv(input_path)
df = df.withColumn("dt", lit(target_date)).withColumn("source_file", input_file_name())

df.write.format("delta").mode("append").partitionBy("dt").save(bronze_path)

# COMMAND ----------

# Step 2: Silver - Clean and transform today's partition
bronze_df = spark.read.format("delta").load(bronze_path).filter(f"dt = '{target_date}'")

silver_df = bronze_df.dropna(subset=["timestamp", "well_id", "pressure_psi", "temperature_celsius"])
silver_df.write.format("delta").mode("append").partitionBy("dt").save(silver_path)

# COMMAND ----------

# Step 3: Gold - Merge Aggregates
from delta.tables import DeltaTable
from pyspark.sql.functions import avg, sum as _sum, max as _max

agg_df = silver_df.groupBy("well_id").agg(
    avg("pressure_psi").alias("avg_pressure_psi"),
    avg("temperature_celsius").alias("avg_temp_celsius"),
    _sum("flow_rate_bpd").alias("total_flow_rate_bpd"),
    _max("gas_detection_ppm").alias("peak_gas_ppm")
)

if DeltaTable.isDeltaTable(spark, gold_path):
    delta_gold = DeltaTable.forPath(spark, gold_path)
    delta_gold.alias("gold").merge(
        agg_df.alias("updates"),
        "gold.well_id = updates.well_id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    agg_df.write.format("delta").save(gold_path)
