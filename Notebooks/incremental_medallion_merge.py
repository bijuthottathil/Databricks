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
bronze_path = "abfss://oillakehouse@lakehouse.dfs.core.windows.net/medallion/bronze/oil_sensor_data"
silver_path = "abfss://oillakehouse@lakehouse.dfs.core.windows.net/medallion/silver/oil_sensor_data_clean"
gold_path = "abfss://oillakehouse@lakehouse.dfs.core.windows.net/medallion/gold/oil_sensor_summary"

# COMMAND ----------

df = spark.read.option("header", True).csv(
  "abfss://lakehouse@oillakehouse.dfs.core.windows.net/medallion/raw/sensor_data/2025-06-18/"
)
df.display()

# COMMAND ----------

input_path = f"abfss://oillakehouse@lakehouse.dfs.core.windows.net/medallion/raw/sensor_data/{target_date}/"

print(input_path)

# COMMAND ----------

from pyspark.sql.functions import input_file_name, lit

# Parameter: ingestion date (e.g., '2025-06-18')
target_date = "2025-06-18"

# Define input path for the date partition in ADLS (external location)
input_path = f"abfss://lakehouse@oillakehouse.dfs.core.windows.net/medallion/raw/sensor_data/{target_date}/"

# Define output Delta location for Bronze layer
bronze_path = "abfss://lakehouse@oillakehouse.dfs.core.windows.net/medallion/bronze/oil_sensor_data"

# Read CSV files for the given date
df = spark.read.option("header", True).csv(input_path)

# Add ingestion metadata columns
df = df.withColumn("dt", lit(target_date)) \
       .withColumn("source_file", input_file_name())

# Append to the Delta table (partitioned by dt)
df.write.format("delta") \
  .mode("append") \
  .partitionBy("dt") \
  .option("mergeSchema", "true").save(bronze_path)

# COMMAND ----------

df = spark.table("oil_sensors_catalog.bronze_schema.oil_sensor_data_raw")
# df.filter("dt = '2025-06-18'").show()
display(df)

# COMMAND ----------



# COMMAND ----------

# Step 2: Silver - Clean and transform today's partition
bronze_df = spark.read.format("delta").load(bronze_path).filter(f"dt = '{target_date}'")

silver_df = bronze_df.dropna(subset=["timestamp", "well_id", "pressure_psi", "temperature_celsius"])
#display(silver_df)
#silver_df.write.format("delta").mode("append").partitionBy("dt").option("mergeSchema", "true").save(silver_path)
#print(silver_path)
silver_df.write.format("delta") \
  .mode("append") \
  .partitionBy("dt") \
  .option("mergeSchema", "true").save(silver_path)


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
