
CREATE OR REFRESH STREAMING LIVE TABLE nytaxi_bronzetogoldtransformaion
COMMENT "Bronze layer"
AS SELECT
    *
FROM STREAM (samples.nyctaxi.trips);

CREATE OR REFRESH STREAMING LIVE TABLE nytaxi_silver_updated
COMMENT "Cleaned Silver layer"
AS
SELECT 
  tpep_pickup_datetime,
  tpep_dropoff_datetime,
  trip_distance,
  fare_amount,
  pickup_zip,
  dropoff_zip
FROM stream(nytaxi_bronzetogoldtransformaion)
WHERE fare_amount > 0 AND trip_distance > 0;

CREATE OR REFRESH STREAMING LIVE TABLE nytaxi_gold_updated
COMMENT "Aggregated metrics for Gold layer"
AS
SELECT
  date_trunc('day', tpep_pickup_datetime) AS trip_date,
  AVG(trip_distance) AS avg_distance,
  SUM(fare_amount) AS total_revenue
FROM stream(nytaxi_silver_updated)
GROUP BY date_trunc('day', tpep_pickup_datetime);

CREATE OR REPLACE MATERIALIZED VIEW nytaxi_gold_view
AS
SELECT
  date_trunc('day', tpep_pickup_datetime) AS trip_date,
  AVG(trip_distance) AS avg_distance,
  SUM(fare_amount) AS total_revenue
FROM nytaxi_silver_updated
GROUP BY date_trunc('day', tpep_pickup_datetime);

