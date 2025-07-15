CREATE OR REFRESH STREAMING LIVE TABLE nytaxi_silver
COMMENT "Cleaned Silver layer"
AS
SELECT 
  tpep_pickup_datetime,
  tpep_dropoff_datetime,
  trip_distance,
  fare_amount,
  pickup_zip,
  dropoff_zip
FROM stream(samples.nyctaxi.trips)
WHERE fare_amount > 0 AND trip_distance > 0

