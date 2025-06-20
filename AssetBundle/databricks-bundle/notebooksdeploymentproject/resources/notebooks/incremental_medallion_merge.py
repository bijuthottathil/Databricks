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

from pyspark.sql.functions import input_file_name, lit

# Parameter: ingestion date (e.g., '2025-06-17')
target_date = "2025-06-15"

# Define input path for the date partition in ADLS (external location)
input_path = f"abfss://lakehouse@oillakehouse.dfs.core.windows.net/medallion/raw/sensor_data/{target_date}"

# Define output Delta location for Bronze layer
bronze_path = "abfss://lakehouse@oillakehouse.dfs.core.windows.net/medallion/bronze/oil_sensor_data"

# Read CSV files for the given date
df = spark.read.option("header", True).csv(input_path, multiLine=True)

# Add ingestion metadata columns
df = df.withColumn("dt", lit(target_date)) \
       .withColumn("source_file", input_file_name())

# Append to the Delta table (partitioned by dt)
df.write.format("delta") \
  .mode("append") \
  .partitionBy("dt") \
  .option("mergeSchema", "true").save(bronze_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM oil_sensors_catalog.bronze_schema.oil_sensor_data_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW bronze_temp AS
# MAGIC SELECT *
# MAGIC FROM oil_sensors_catalog.bronze_schema.oil_sensor_data_raw
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW silver_temp AS
# MAGIC SELECT *
# MAGIC FROM oil_sensors_catalog.bronze_schema.oil_sensor_data_raw
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG oil_sensors_catalog;
# MAGIC USE SCHEMA silver_schema;

# COMMAND ----------

# MAGIC %sql
# MAGIC     
# MAGIC CREATE TABLE IF NOT EXISTS silver_schema.oil_sensor_data_clean
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://lakehouse@oillakehouse.dfs.core.windows.net/medallion/silver/oil_sensor_data_clean';
# MAGIC

# COMMAND ----------

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO silver_schema.oil_sensor_data_clean
# MAGIC SELECT * FROM silver_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_schema.oil_sensor_data_clean

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW bronze_temp AS
# MAGIC SELECT * 
# MAGIC FROM delta.`abfss://lakehouse@oillakehouse.dfs.core.windows.net/medallion/bronze/oil_sensor_data`
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG oil_sensors_catalog;
# MAGIC USE SCHEMA gold_schema;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW agg_temp_view AS
# MAGIC SELECT
# MAGIC     well_id,
# MAGIC     AVG(pressure_psi) AS avg_pressure_psi,
# MAGIC     AVG(temperature_celsius) AS avg_temp_celsius,
# MAGIC     SUM(flow_rate_bpd) AS total_flow_rate_bpd,
# MAGIC     MAX(gas_detection_ppm) AS peak_gas_ppm
# MAGIC FROM silver_schema.oil_sensor_data_clean
# MAGIC GROUP BY well_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from agg_temp_view

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from oil_sensors_catalog.gold_schema.oil_sensor_metrics

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO oil_sensors_catalog.gold_schema.oil_sensor_metrics AS target
# MAGIC USING agg_temp_view AS source
# MAGIC ON target.well_id = source.well_id
# MAGIC
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     target.avg_pressure_psi = source.avg_pressure_psi,
# MAGIC     target.avg_temp_celsius = source.avg_temp_celsius,
# MAGIC     target.total_flow_rate_bpd = source.total_flow_rate_bpd,
# MAGIC     target.peak_gas_ppm = source.peak_gas_ppm
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     well_id,
# MAGIC     avg_pressure_psi,
# MAGIC     avg_temp_celsius,
# MAGIC     total_flow_rate_bpd,
# MAGIC     peak_gas_ppm
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     source.well_id,
# MAGIC     source.avg_pressure_psi,
# MAGIC     source.avg_temp_celsius,
# MAGIC     source.total_flow_rate_bpd,
# MAGIC     source.peak_gas_ppm
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC select *  FROM agg_temp_view

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from oil_sensors_catalog.gold_schema.oil_sensor_metrics
