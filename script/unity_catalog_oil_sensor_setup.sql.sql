-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Unity Catalog Setup for Oil Sensor Medallion Architecture

-- COMMAND ----------

-- Step 1: Create External Location (update <storage_account> and credential name)
CREATE EXTERNAL LOCATION IF NOT EXISTS oil_ext_loc
URL 'abfss://oil-lakehouse@<storage_account>.dfs.core.windows.net/medallion'
WITH (STORAGE CREDENTIAL my_adls_credential);

-- COMMAND ----------

-- Step 2: Create Catalog and Schemas
CREATE CATALOG IF NOT EXISTS oil_sensors_catalog;
USE CATALOG oil_sensors_catalog;

CREATE SCHEMA IF NOT EXISTS bronze_schema;
CREATE SCHEMA IF NOT EXISTS silver_schema;
CREATE SCHEMA IF NOT EXISTS gold_schema;

-- COMMAND ----------

-- Step 3: Create External Tables
-- Bronze
CREATE TABLE IF NOT EXISTS bronze_schema.oil_sensor_data_raw
USING DELTA
LOCATION 'abfss://oil-lakehouse@<storage_account>.dfs.core.windows.net/medallion/bronze/oil_sensor_data';

-- Silver
CREATE TABLE IF NOT EXISTS silver_schema.oil_sensor_data_clean
USING DELTA
LOCATION 'abfss://oil-lakehouse@<storage_account>.dfs.core.windows.net/medallion/silver/oil_sensor_data_clean';

-- Gold
CREATE TABLE IF NOT EXISTS gold_schema.oil_sensor_summary
USING DELTA
LOCATION 'abfss://oil-lakehouse@<storage_account>.dfs.core.windows.net/medallion/gold/oil_sensor_summary';
