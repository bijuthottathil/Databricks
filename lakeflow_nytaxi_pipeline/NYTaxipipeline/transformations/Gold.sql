CREATE OR REFRESH STREAMING LIVE TABLE nytaxi_gold
COMMENT "Aggregated metrics for Gold layer"
AS
SELECT
  date_trunc('day', tpep_pickup_datetime) AS trip_date,
  AVG(trip_distance) AS avg_distance,
  SUM(fare_amount) AS total_revenue
FROM stream(nytaxi_silver)
GROUP BY date_trunc('day', tpep_pickup_datetime);




